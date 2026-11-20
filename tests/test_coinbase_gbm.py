import unittest
from coinbase_gbm import calculate_coinbase_gbm_baseline


class CoinbaseGbmTests(unittest.TestCase):
    def make_candles(self, closes):
        return [
            {"time": 1_800_000_000 + i * 60, "close": close}
            for i, close in enumerate(closes)
        ]

    def test_insufficient_history_is_not_a_probability(self):
        result = calculate_coinbase_gbm_baseline(
            100, 100, 900, self.make_candles([100 + i * 0.01 for i in range(10)])
        )
        self.assertEqual(result["coinbase_gbm_status"], "HISTORIAL_INSUFICIENTE")
        self.assertIsNone(result["coinbase_gbm_yes_probability"])

    def test_valid_history_returns_complementary_probabilities(self):
        closes = [100 + (i % 5) * 0.03 + i * 0.001 for i in range(40)]
        result = calculate_coinbase_gbm_baseline(
            100.2, 100, 600, self.make_candles(closes)
        )
        self.assertEqual(result["coinbase_gbm_status"], "OK")
        self.assertAlmostEqual(
            result["coinbase_gbm_yes_probability"] + result["coinbase_gbm_no_probability"],
            100.0, places=1
        )

    def test_gap_is_not_counted_as_a_one_minute_return(self):
        candles = self.make_candles([100 + i * 0.01 for i in range(15)])
        candles += [
            {"time": 1_800_000_000 + 20 * 60 + i * 60, "close": 101 + i * 0.01}
            for i in range(15)
        ]
        result = calculate_coinbase_gbm_baseline(100.5, 100, 600, candles)
        self.assertEqual(result["coinbase_gbm_volatility_samples"], 28)


if __name__ == "__main__":
    unittest.main()
