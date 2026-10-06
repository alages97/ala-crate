// SPDX-License-Identifier: MIT
pragma solidity 0.8.30;

import {IERC20Replay, KuruCrystalAtomicExecutor} from "../contracts/KuruCrystalAtomicExecutor.sol";

interface VmReplay {
    function createSelectFork(string calldata rpcUrl, uint256 blockNumber) external returns (uint256 forkId);
    function envOr(string calldata name, string calldata defaultValue) external returns (string memory value);
    function load(address target, bytes32 slot) external view returns (bytes32 data);
    function prank(address msgSender) external;
    function roll(uint256 newHeight) external;
}

/// @notice Historical fork replay for every >=5 bps screen in
///         kuru-crystal-shadow-multiblock-20260928T1234Z.ndjson.
///
/// Each case forks the state immediately after the captured block and then
/// moves only the synthetic block number to N+1. Deploy, funding and route
/// calls therefore occur in a fresh insertion position without importing any
/// state transition from the real N+1 block. A fresh fork per case prevents
/// one replay from consuming another replay's liquidity.
contract KuruCrystalForkReplayTest {
    VmReplay private constant vm = VmReplay(address(uint160(uint256(keccak256("hevm cheat code")))));

    address private constant KURU_MARKET = 0x065C9d28E428A0db40191a54d33d5b7c71a9C394;
    address private constant USDC = 0x754704Bc059F8C67012fEd69BC8A327a5aafb603;
    address private constant USDC_WHALE = 0xe1Aa0010B4c25F38A7c8A724FdD79c6e8cE543fe;
    address private constant EXPECTED_KURU_IMPLEMENTATION = 0x5e3446c600524Be453bbCEFD46a9E4C9bE8899a0;

    bytes32 private constant EIP1967_IMPLEMENTATION_SLOT =
        0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc;
    bytes32 private constant EXPECTED_KURU_IMPLEMENTATION_CODEHASH =
        0x6576ced90ca537c9aa058ea4a54ab5edd802ab78b834168b21fcc6d9ac392c0c;

    // 0 = Kuru buy -> Crystal sell; 1 = Crystal buy -> Kuru sell.
    struct Candidate {
        uint64 anchorBlock;
        bytes32 anchorHash;
        uint64 notionalUsdcRaw;
        uint8 direction;
        int64 capturedEdgeBpsE5;
    }

    event ReplayResult(
        uint256 indexed candidateIndex,
        uint64 indexed anchorBlock,
        bytes32 indexed anchorHash,
        uint64 stateBlock,
        uint64 simulatedBlock,
        uint8 direction,
        uint64 notionalUsdcRaw,
        int64 capturedEdgeBpsE5,
        bool callSucceeded,
        bytes4 revertSelector,
        uint256 gasUsed,
        uint256 initialUsdcRaw,
        uint256 observedFinalUsdcRaw,
        int256 observedPnlUsdcRaw,
        bytes returnOrRevertData
    );

    string private rpcUrl;

    function setUp() external {
        rpcUrl = vm.envOr("MONAD_RPC_URL", string("https://rpc.monad.xyz"));
    }

    function testReplayStrongestCandidate() external {
        Candidate memory candidate = _candidate(14);
        _replay(14, candidate, candidate.anchorBlock, true);
    }

    function testReplayAllSixteenCandidateScreens() external {
        for (uint256 index = 0; index < 16; ++index) {
            Candidate memory candidate = _candidate(index);
            _replay(index, candidate, candidate.anchorBlock, true);
        }
    }

    /// @notice Replays after importing the real N+1 block state. This is a
    ///         conservative one-block-latency scenario, distinct from the
    ///         optimistic insertion-position replay above.
    function testReplayAllAtObservedNextBlockState() external {
        for (uint256 index = 0; index < 16; ++index) {
            Candidate memory candidate = _candidate(index);
            _replay(index, candidate, candidate.anchorBlock + 1, false);
        }
    }

    function _replay(uint256 index, Candidate memory candidate, uint64 stateBlock, bool requireCapturedResult) private {
        vm.createSelectFork(rpcUrl, stateBlock);

        // Advance only the local environment, preserving the selected fork's
        // post-state as an explicit synthetic insertion position.
        vm.roll(uint256(stateBlock) + 1);
        require(this.readBlockHash(candidate.anchorBlock) == candidate.anchorHash, "anchor hash mismatch");
        KuruCrystalAtomicExecutor executor = new KuruCrystalAtomicExecutor(address(this));
        _requirePinnedKuruImplementation();
        _fundExecutor(address(executor), candidate.notionalUsdcRaw);

        uint256 initialUsdc = IERC20Replay(USDC).balanceOf(address(executor));
        require(initialUsdc == candidate.notionalUsdcRaw, "unexpected replay funding");

        bytes memory calldata_;
        if (candidate.direction == 0) {
            calldata_ = abi.encodeCall(executor.executeKuruBuyCrystalSell, (candidate.notionalUsdcRaw, 0, 0, 0));
        } else {
            calldata_ = abi.encodeCall(executor.executeCrystalBuyKuruSell, (candidate.notionalUsdcRaw, 0, 0, 0));
        }

        uint256 gasBefore = gasleft();
        (bool succeeded, bytes memory result) = address(executor).call(calldata_);
        uint256 gasUsed = gasBefore - gasleft();
        bytes4 selector = succeeded ? bytes4(0) : _selector(result);

        uint256 observedFinal = IERC20Replay(USDC).balanceOf(address(executor));
        if (!succeeded && selector == KuruCrystalAtomicExecutor.NotProfitable.selector) {
            (, observedFinal,) = _decodeNotProfitable(result);
        }
        int256 observedPnl = _signedDifference(observedFinal, initialUsdc);

        emit ReplayResult(
            index,
            candidate.anchorBlock,
            candidate.anchorHash,
            stateBlock,
            stateBlock + 1,
            candidate.direction,
            candidate.notionalUsdcRaw,
            candidate.capturedEdgeBpsE5,
            succeeded,
            selector,
            gasUsed,
            initialUsdc,
            observedFinal,
            observedPnl,
            result
        );

        if (requireCapturedResult) {
            require(succeeded, "captured route did not execute atomically");
            require(candidate.capturedEdgeBpsE5 > 0, "captured edge must be positive");
            uint256 scaledPnl = uint256(candidate.notionalUsdcRaw) * uint64(candidate.capturedEdgeBpsE5);
            require(scaledPnl % 1e9 == 0, "captured PnL is not exactly representable");
            uint256 expectedPnl = scaledPnl / 1e9;
            require(observedFinal == initialUsdc + expectedPnl, "atomic PnL changed from capture");
            require(abi.decode(result, (uint256)) == expectedPnl, "executor return PnL mismatch");
        }
    }

    function _fundExecutor(address executor, uint256 amount) private {
        require(USDC_WHALE.code.length == 0, "funding source is no longer an EOA");
        vm.prank(USDC_WHALE);
        (bool ok, bytes memory result) =
            USDC.call(abi.encodeWithSelector(bytes4(keccak256("transfer(address,uint256)")), executor, amount));
        require(ok && (result.length == 0 || abi.decode(result, (bool))), "USDC funding failed");
    }

    function readBlockHash(uint256 blockNumber) external view returns (bytes32) {
        require(msg.sender == address(this), "test-only helper");
        return blockhash(blockNumber);
    }

    function _requirePinnedKuruImplementation() private view {
        bytes32 raw = vm.load(KURU_MARKET, EIP1967_IMPLEMENTATION_SLOT);
        address implementation = address(uint160(uint256(raw)));
        require(implementation == EXPECTED_KURU_IMPLEMENTATION, "Kuru implementation changed");
        require(implementation.codehash == EXPECTED_KURU_IMPLEMENTATION_CODEHASH, "Kuru implementation code changed");
    }

    function _selector(bytes memory data) private pure returns (bytes4 selector) {
        if (data.length < 4) return bytes4(0);
        assembly ("memory-safe") {
            selector := mload(add(data, 0x20))
        }
    }

    function _decodeNotProfitable(bytes memory data)
        private
        pure
        returns (uint256 beforeBalance, uint256 afterBalance, uint256 minProfit)
    {
        require(data.length >= 100, "short NotProfitable payload");
        assembly ("memory-safe") {
            beforeBalance := mload(add(data, 0x24))
            afterBalance := mload(add(data, 0x44))
            minProfit := mload(add(data, 0x64))
        }
    }

    function _signedDifference(uint256 lhs, uint256 rhs) private pure returns (int256 result) {
        if (lhs >= rhs) return int256(lhs - rhs);
        return -int256(rhs - lhs);
    }

    function _candidate(uint256 index) private pure returns (Candidate memory candidate) {
        if (index == 0) {
            return Candidate(
                108752344, 0x3023cfc25b27bed9e6d05eea0e7ec050dc47fc42709adcd7975f50d8a0576180, 100_000_000, 0, 3_157_430
            );
        }
        if (index == 1) {
            return Candidate(
                108752344, 0x3023cfc25b27bed9e6d05eea0e7ec050dc47fc42709adcd7975f50d8a0576180, 500_000_000, 0, 2_172_010
            );
        }
        if (index == 2) {
            return Candidate(
                108752347, 0x916291c10078fdfdc37dd466fbcd736d42ea3d8b4083f979aa8bd8095d0ecaed, 100_000_000, 0, 1_703_150
            );
        }
        if (index == 3) {
            return Candidate(
                108752372, 0x1b46a627c1e703562c89ef4b032a08e6e0036b8efe64d6ed3db7f6decbf0c193, 100_000_000, 0, 929_970
            );
        }
        if (index == 4) {
            return Candidate(
                108752420, 0x13e121260b57f3cb7b38faf74cc2c6a81f215a8077bcb671e6bf118a52c2f43f, 100_000_000, 1, 1_174_890
            );
        }
        if (index == 5) {
            return Candidate(
                108752440, 0x68c93a19baa13503797115ce4d612121dbfce612b6435dd5a9652183820fcdc3, 100_000_000, 1, 1_243_760
            );
        }
        if (index == 6) {
            return Candidate(
                108752537, 0x1cd65402aae5620d923bb8212170751a0abf582c68b7cac6886ba7f7827a84a4, 100_000_000, 0, 1_097_030
            );
        }
        if (index == 7) {
            return Candidate(
                108752721, 0x61953990c61c9ce0ebc04caad1d6f23581808dca5d8da28adb028b7ca96d299c, 100_000_000, 1, 1_936_520
            );
        }
        if (index == 8) {
            return Candidate(
                108752721, 0x61953990c61c9ce0ebc04caad1d6f23581808dca5d8da28adb028b7ca96d299c, 500_000_000, 1, 1_091_890
            );
        }
        if (index == 9) {
            return Candidate(
                108752801, 0x274763f92a3ddfd8ff58606dd14ed2dc50f758976417a11ddb20239698fba6cd, 100_000_000, 1, 1_196_710
            );
        }
        if (index == 10) {
            return Candidate(
                108752901, 0x032e062b71cf0126b43daea13ed2000a65cc1f6ead5b6cd09cbaf528ab09db35, 100_000_000, 0, 1_200_290
            );
        }
        if (index == 11) {
            return Candidate(
                108752960, 0x4782f3961ab4c1688a89b532edb2f8a8a7c53225df8798fe4af88c43f77241b8, 100_000_000, 1, 959_830
            );
        }
        if (index == 12) {
            return Candidate(
                108752996, 0x9a7330daf386ac7fa898802b422c26ab9caef9252c60af94fcca7009db137ba7, 100_000_000, 1, 515_800
            );
        }
        if (index == 13) {
            return Candidate(
                108753083, 0x301b7d9b14b2168eba793a50dc901a981f1390a73528d38d7e7cfd41a9b2a39c, 100_000_000, 0, 586_570
            );
        }
        if (index == 14) {
            return Candidate(
                108753128, 0x72802cf763eb096d21439ab3713d7b043b9f635c537e53f5643dd4859da236df, 100_000_000, 0, 4_012_530
            );
        }
        if (index == 15) {
            return Candidate(
                108753128, 0x72802cf763eb096d21439ab3713d7b043b9f635c537e53f5643dd4859da236df, 500_000_000, 0, 1_945_318
            );
        }
        revert("candidate index out of range");
    }
}
