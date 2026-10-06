// SPDX-License-Identifier: MIT
pragma solidity 0.8.30;

/// @notice The small ERC-20 surface used by the replay executor.
interface IERC20Replay {
    function approve(address spender, uint256 amount) external returns (bool);
    function balanceOf(address account) external view returns (uint256);
}

interface IWrappedMonReplay is IERC20Replay {
    function deposit() external payable;
    function withdraw(uint256 amount) external;
}

interface IKuruReplay {
    function getMarketParams()
        external
        view
        returns (
            uint32 pricePrecision,
            uint96 sizePrecision,
            address baseAsset,
            uint256 baseAssetDecimals,
            address quoteAsset,
            uint256 quoteAssetDecimals,
            uint32 tickSize,
            uint96 minSize,
            uint96 maxSize,
            uint256 takerFeeBps,
            uint256 makerFeeBps
        );

    function placeAndExecuteMarketBuy(uint96 quoteSize, uint256 minAmountOut, bool isMargin, bool isFillOrKill)
        external
        payable
        returns (uint256 baseTokensCredited);

    function placeAndExecuteMarketSell(uint96 size, uint256 minAmountOut, bool isMargin, bool isFillOrKill)
        external
        payable
        returns (uint256 quoteCredited);
}

interface ICrystalReplay {
    function marketOrder(
        address market,
        bool isBuy,
        bool isExactInput,
        uint256 options,
        uint256 orderType,
        uint256 size,
        uint256 worstPrice,
        address referrer,
        address user
    ) external returns (uint256 amountIn, uint256 amountOut, uint256 id);

    function getDepositedBalance(address user, address asset)
        external
        view
        returns (uint256 available, uint256 locked, uint256 unsettled);
}

/// @notice Purpose-built, owner-operated executor for historical Kuru/Crystal
///         MON-USDC replay. This is intentionally not a generic production
///         router: every address and market invariant is pinned to the capture.
contract KuruCrystalAtomicExecutor {
    address public constant KURU_MARKET = 0x065C9d28E428A0db40191a54d33d5b7c71a9C394;
    address public constant CRYSTAL_SINGLETON = 0x508254c838B2e936B0631440c5C6E3AB3a4a98BD;
    address public constant CRYSTAL_MARKET = 0x39fAE95717cfD4bdA22317F1c124660A166b6BEc;
    address public constant USDC = 0x754704Bc059F8C67012fEd69BC8A327a5aafb603;
    address public constant WMON = 0x3bd359C1119dA7Da1D913D1C4D2B7c461115433A;

    bytes32 public constant EXPECTED_KURU_PROXY_CODEHASH =
        0xb3fcfca704395c210e969b841f35fff7483f99721de8d65b219615dd5e03f43b;
    bytes32 public constant EXPECTED_CRYSTAL_SINGLETON_CODEHASH =
        0x87db1b1988e87f94e2cf20519e32d8416400681a71f6cf54c7d48a8a34e1f67a;
    bytes32 public constant EXPECTED_CRYSTAL_MARKET_CODEHASH =
        0x6d1517d36a81f00763d4377e8310ed4fec67f7773fb18a0b4722ec86b139ab8c;

    uint256 private constant MONAD_CHAIN_ID = 143;
    uint256 private constant USDC_SCALE = 1e6;
    uint256 private constant MON_SCALE = 1e18;
    uint256 private constant KURU_RAW_MON_PER_SIZE_UNIT = 1e8;
    uint256 private constant CRYSTAL_EXTERNAL_BALANCE_OPTIONS = 0;
    uint256 private constant CRYSTAL_COMPLETE_FILL_ORDER = 1;

    error NotOwner(address caller);
    error InvalidOwner();
    error ReentrantCall();
    error WrongChain(uint256 actual);
    error CodeHashMismatch(address target, bytes32 expected, bytes32 actual);
    error MarketInvariantMismatch();
    error InsufficientExecutorUsdc(uint256 required, uint256 available);
    error AmountDoesNotFitUint96(uint256 amount);
    error AmountNotExactlyRepresentable(uint256 amount);
    error UnexpectedNativeDelta(uint256 expected, uint256 actual);
    error UnexpectedTokenDelta(address token, uint256 expected, uint256 actual);
    error CrystalPartialFill(uint256 requested, uint256 consumed);
    error CrystalRestingOrder(uint256 id);
    error CrystalInternalBalance(address asset, uint256 available, uint256 locked, uint256 unsettled);
    error OutputBelowMinimum(uint256 minimum, uint256 actual);
    error TokenCallFailed(address token, bytes4 selector);
    error NotProfitable(uint256 beforeBalance, uint256 afterBalance, uint256 minProfit);
    error ResidualInvariant(uint256 expected, uint256 actual);

    event RouteExecuted(
        uint8 indexed direction,
        uint256 usdcInput,
        uint256 firstLegOutput,
        uint256 secondLegOutput,
        uint256 finalUsdc,
        uint256 nativeDust,
        uint256 profit
    );

    address public immutable owner;
    uint32 public immutable kuruPricePrecision;
    uint96 public immutable kuruSizePrecision;
    bool private entered;

    constructor(address owner_) {
        if (owner_ == address(0)) revert InvalidOwner();
        if (block.chainid != MONAD_CHAIN_ID) revert WrongChain(block.chainid);
        _requireCodeHash(KURU_MARKET, EXPECTED_KURU_PROXY_CODEHASH);
        _requireCodeHash(CRYSTAL_SINGLETON, EXPECTED_CRYSTAL_SINGLETON_CODEHASH);
        _requireCodeHash(CRYSTAL_MARKET, EXPECTED_CRYSTAL_MARKET_CODEHASH);

        (
            uint32 pricePrecision,
            uint96 sizePrecision,
            address baseAsset,
            uint256 baseDecimals,
            address quoteAsset,
            uint256 quoteDecimals,
            uint32 tickSize,
            uint96 minSize,
            uint96 maxSize,
            uint256 takerFeeBps,
            uint256 makerFeeBps
        ) = IKuruReplay(KURU_MARKET).getMarketParams();
        if (
            pricePrecision != 1e8 || sizePrecision != 1e10 || baseAsset != address(0) || baseDecimals != 18
                || quoteAsset != USDC || quoteDecimals != 6 || tickSize != 100 || minSize != 2e12 || maxSize != 2e18
                || takerFeeBps != 0 || makerFeeBps != 0
        ) revert MarketInvariantMismatch();

        owner = owner_;
        kuruPricePrecision = pricePrecision;
        kuruSizePrecision = sizePrecision;
    }

    modifier onlyOwner() {
        if (msg.sender != owner) revert NotOwner(msg.sender);
        _;
    }

    modifier nonReentrant() {
        if (entered) revert ReentrantCall();
        entered = true;
        _;
        entered = false;
    }

    receive() external payable {}

    /// @notice Buy native MON on Kuru, wrap it, then sell WMON on Crystal.
    /// @dev The executor must already hold `usdcInput`. Both exchange legs use
    ///      their complete-fill modes and the whole call reverts unless the
    ///      requested minimum profit is retained in USDC.
    function executeKuruBuyCrystalSell(
        uint256 usdcInput,
        uint256 minMonOut,
        uint256 minCrystalUsdcOut,
        uint256 minProfit
    ) external nonReentrant onlyOwner returns (uint256 profit) {
        uint256 usdcBefore = _balanceOf(USDC);
        if (usdcBefore < usdcInput) {
            revert InsufficientExecutorUsdc(usdcInput, usdcBefore);
        }
        uint256 nativeBefore = address(this).balance;
        uint256 wmonBefore = _balanceOf(WMON);
        _requireNoCrystalBalance(USDC);
        _requireNoCrystalBalance(WMON);

        uint96 quoteUnits = _kuruQuoteUnits(usdcInput);
        _forceApprove(USDC, KURU_MARKET, usdcInput);
        uint256 monOut = IKuruReplay(KURU_MARKET).placeAndExecuteMarketBuy(quoteUnits, minMonOut, false, true);
        _forceApprove(USDC, KURU_MARKET, 0);

        uint256 nativeDelta = address(this).balance - nativeBefore;
        if (nativeDelta != monOut) revert UnexpectedNativeDelta(monOut, nativeDelta);
        IWrappedMonReplay(WMON).deposit{value: monOut}();
        uint256 wrappedDelta = _balanceOf(WMON) - wmonBefore;
        if (wrappedDelta != monOut) revert UnexpectedTokenDelta(WMON, monOut, wrappedDelta);

        _forceApprove(WMON, CRYSTAL_SINGLETON, monOut);
        (uint256 crystalIn, uint256 crystalOut, uint256 orderId) = ICrystalReplay(CRYSTAL_SINGLETON)
            .marketOrder(
                CRYSTAL_MARKET,
                false,
                true,
                CRYSTAL_EXTERNAL_BALANCE_OPTIONS,
                CRYSTAL_COMPLETE_FILL_ORDER,
                monOut,
                0,
                address(0),
                address(this)
            );
        _forceApprove(WMON, CRYSTAL_SINGLETON, 0);
        _requireCrystalCompletion(monOut, crystalIn, orderId);
        if (crystalOut < minCrystalUsdcOut) {
            revert OutputBelowMinimum(minCrystalUsdcOut, crystalOut);
        }

        uint256 nativeAfter = address(this).balance;
        uint256 wmonAfter = _balanceOf(WMON);
        if (nativeAfter != nativeBefore) revert ResidualInvariant(nativeBefore, nativeAfter);
        if (wmonAfter != wmonBefore) revert ResidualInvariant(wmonBefore, wmonAfter);
        _requireNoCrystalBalance(USDC);
        _requireNoCrystalBalance(WMON);

        uint256 usdcAfter = _balanceOf(USDC);
        profit = _requireProfit(usdcBefore, usdcAfter, minProfit);
        emit RouteExecuted(0, usdcInput, monOut, crystalOut, usdcAfter, 0, profit);
    }

    /// @notice Buy WMON on Crystal, unwrap it, then sell native MON on Kuru.
    /// @dev Sub-size-precision MON dust is retained and reported by the event.
    function executeCrystalBuyKuruSell(uint256 usdcInput, uint256 minWmonOut, uint256 minKuruUsdcOut, uint256 minProfit)
        external
        nonReentrant
        onlyOwner
        returns (uint256 profit)
    {
        uint256 usdcBefore = _balanceOf(USDC);
        if (usdcBefore < usdcInput) {
            revert InsufficientExecutorUsdc(usdcInput, usdcBefore);
        }
        uint256 nativeBefore = address(this).balance;
        uint256 wmonBefore = _balanceOf(WMON);
        _requireNoCrystalBalance(USDC);
        _requireNoCrystalBalance(WMON);

        _forceApprove(USDC, CRYSTAL_SINGLETON, usdcInput);
        (uint256 crystalIn, uint256 wmonOut, uint256 orderId) = ICrystalReplay(CRYSTAL_SINGLETON)
            .marketOrder(
                CRYSTAL_MARKET,
                true,
                true,
                CRYSTAL_EXTERNAL_BALANCE_OPTIONS,
                CRYSTAL_COMPLETE_FILL_ORDER,
                usdcInput,
                0,
                address(0),
                address(this)
            );
        _forceApprove(USDC, CRYSTAL_SINGLETON, 0);
        _requireCrystalCompletion(usdcInput, crystalIn, orderId);
        if (wmonOut < minWmonOut) revert OutputBelowMinimum(minWmonOut, wmonOut);

        uint256 wrappedDelta = _balanceOf(WMON) - wmonBefore;
        if (wrappedDelta != wmonOut) revert UnexpectedTokenDelta(WMON, wmonOut, wrappedDelta);
        IWrappedMonReplay(WMON).withdraw(wmonOut);

        (uint96 sizeUnits, uint256 representedNative, uint256 dust) = _kuruSizeUnits(wmonOut);
        uint256 kuruOut = IKuruReplay(KURU_MARKET).placeAndExecuteMarketSell{value: representedNative}(
            sizeUnits, minKuruUsdcOut, false, true
        );

        uint256 wmonAfter = _balanceOf(WMON);
        uint256 nativeAfter = address(this).balance;
        if (wmonAfter != wmonBefore) revert ResidualInvariant(wmonBefore, wmonAfter);
        if (nativeAfter != nativeBefore + dust) {
            revert ResidualInvariant(nativeBefore + dust, nativeAfter);
        }
        _requireNoCrystalBalance(USDC);
        _requireNoCrystalBalance(WMON);

        uint256 usdcAfter = _balanceOf(USDC);
        profit = _requireProfit(usdcBefore, usdcAfter, minProfit);
        emit RouteExecuted(1, usdcInput, wmonOut, kuruOut, usdcAfter, dust, profit);
    }

    function _kuruQuoteUnits(uint256 usdcRaw) private view returns (uint96 units) {
        uint256 numerator = usdcRaw * uint256(kuruPricePrecision);
        if (numerator % USDC_SCALE != 0) revert AmountNotExactlyRepresentable(usdcRaw);
        uint256 converted = numerator / USDC_SCALE;
        if (converted > type(uint96).max) revert AmountDoesNotFitUint96(converted);
        // forge-lint: disable-next-line(unsafe-typecast)
        units = uint96(converted); // Bounds checked immediately above.
    }

    function _kuruSizeUnits(uint256 monRaw) private view returns (uint96 units, uint256 representedRaw, uint256 dust) {
        uint256 converted = monRaw * uint256(kuruSizePrecision) / MON_SCALE;
        if (converted == 0 || converted > type(uint96).max) {
            revert AmountDoesNotFitUint96(converted);
        }
        // forge-lint: disable-next-line(unsafe-typecast)
        units = uint96(converted); // Bounds checked immediately above.
        representedRaw = converted * KURU_RAW_MON_PER_SIZE_UNIT;
        dust = monRaw - representedRaw;
    }

    function _requireProfit(uint256 beforeBalance, uint256 afterBalance, uint256 minProfit)
        private
        pure
        returns (uint256 profit)
    {
        if (afterBalance < beforeBalance || afterBalance - beforeBalance < minProfit) {
            revert NotProfitable(beforeBalance, afterBalance, minProfit);
        }
        profit = afterBalance - beforeBalance;
    }

    function _requireCrystalCompletion(uint256 requested, uint256 consumed, uint256 orderId) private pure {
        if (consumed != requested) revert CrystalPartialFill(requested, consumed);
        if (orderId != 0) revert CrystalRestingOrder(orderId);
    }

    function _requireNoCrystalBalance(address asset) private view {
        (uint256 available, uint256 locked, uint256 unsettled) =
            ICrystalReplay(CRYSTAL_SINGLETON).getDepositedBalance(address(this), asset);
        if (available != 0 || locked != 0 || unsettled != 0) {
            revert CrystalInternalBalance(asset, available, locked, unsettled);
        }
    }

    function _balanceOf(address token) private view returns (uint256 result) {
        (bool ok, bytes memory data) =
            token.staticcall(abi.encodeWithSelector(IERC20Replay.balanceOf.selector, address(this)));
        if (!ok || data.length < 32) revert TokenCallFailed(token, IERC20Replay.balanceOf.selector);
        result = abi.decode(data, (uint256));
    }

    function _forceApprove(address token, address spender, uint256 amount) private {
        _safeApprove(token, spender, 0);
        if (amount != 0) _safeApprove(token, spender, amount);
    }

    function _safeApprove(address token, address spender, uint256 amount) private {
        (bool ok, bytes memory data) =
            token.call(abi.encodeWithSelector(IERC20Replay.approve.selector, spender, amount));
        if (!ok || (data.length != 0 && !abi.decode(data, (bool)))) {
            revert TokenCallFailed(token, IERC20Replay.approve.selector);
        }
    }

    function _requireCodeHash(address target, bytes32 expected) private view {
        bytes32 actual = target.codehash;
        if (actual != expected) revert CodeHashMismatch(target, expected, actual);
    }
}
