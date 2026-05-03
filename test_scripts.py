"""
Unit tests for all scripts in the chess analysis bot project.

Run with:
    python -m unittest test_scripts.py -v

Or to run a single test class:
    python -m unittest test_scripts.TestExtractionWorkers -v
"""

import io
import json
import os
import shutil
import sys
import tempfile
import unittest
from unittest import mock

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


# A short, well-formed game (3 full moves = 6 plies) used by every ETL worker test.
# The lichess pipeline feeds chess.pgn.read_game a moves-only string, so we mirror that.
SAMPLE_GAME_INPUT = (
    "1. e4 e5 2. Nf3 Nc6 3. Bb5 a6 1-0",
    "Rated Blitz game",
    1500,
    1450,
)
EXPECTED_PLIES = 6


# =========================================================
# app.py
# =========================================================
class TestApp(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import app
        cls.module = app
        cls.client = app.app.test_client()

    def test_get_base_fen_keeps_first_four_fields(self):
        import chess
        base = self.module.get_base_fen(chess.Board())
        parts = base.split(" ")
        self.assertEqual(len(parts), 4)
        self.assertEqual(parts[0], "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR")
        self.assertEqual(parts[1], "w")

    def test_setup_board_with_valid_moves(self):
        response = self.client.post("/setup_board", json={"moves": "e4 e5 Nf3"})
        self.assertEqual(response.status_code, 200)
        fen = response.get_json()["fen"]
        # After 1. e4 e5 2. Nf3 it's Black to move
        self.assertIn(" b ", fen)

    def test_setup_board_empty_moves_returns_starting_position(self):
        import chess
        response = self.client.post("/setup_board", json={"moves": ""})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["fen"], chess.Board().fen())

    def test_setup_board_invalid_moves_returns_400(self):
        response = self.client.post("/setup_board", json={"moves": "garbage notamove"})
        self.assertEqual(response.status_code, 400)
        self.assertIn("error", response.get_json())

    def test_get_opponent_move_response_shape(self):
        # Outcome depends on whether monthly_databases/ contains matching data.
        # We assert the contract: 200 returns {move, alternatives}, 404 returns {error}.
        response = self.client.post(
            "/get_opponent_move",
            json={
                "fen": "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1",
                "elo_min": 1000,
                "elo_max": 2000,
            },
        )
        self.assertIn(response.status_code, (200, 404))
        body = response.get_json()
        if response.status_code == 200:
            self.assertIn("move", body)
            self.assertIn("alternatives", body)
            self.assertIsInstance(body["alternatives"], list)
            self.assertLessEqual(len(body["alternatives"]), 5)
        else:
            self.assertIn("error", body)


# =========================================================
# ETL Stage 1: all four implementations + auto-update + benchmark
# All variants share an identical per-game parser; assert they agree.
# =========================================================
class TestExtractionWorkers(unittest.TestCase):
    def _assert_records_valid(self, positions):
        self.assertEqual(len(positions), EXPECTED_PLIES)
        for rec in positions:
            self.assertEqual(set(rec.keys()), {"Event", "PlayerEloBucket", "BaseFEN", "MoveSAN"})
            self.assertEqual(rec["Event"], "Rated Blitz game")

        # White (1500) plays plies 0, 2, 4 — bucket 1500
        # Black (1450) plays plies 1, 3, 5 — bucket 1400
        self.assertEqual(positions[0]["MoveSAN"], "e4")
        self.assertEqual(positions[0]["PlayerEloBucket"], 1500)
        self.assertEqual(positions[1]["MoveSAN"], "e5")
        self.assertEqual(positions[1]["PlayerEloBucket"], 1400)
        self.assertEqual(positions[5]["MoveSAN"], "a6")
        self.assertEqual(positions[5]["PlayerEloBucket"], 1400)

    def test_parallel(self):
        from lichess_etl_1_extraction_parallel import process_single_game
        self._assert_records_valid(process_single_game(SAMPLE_GAME_INPUT))

    def test_dask(self):
        try:
            from lichess_etl_1_extraction_dask import process_single_game
        except ImportError as e:
            self.skipTest(f"dask not available: {e}")
        self._assert_records_valid(process_single_game(SAMPLE_GAME_INPUT))

    def test_c_optimized(self):
        try:
            from lichess_etl_1_extraction_c_optimized import process_single_game
        except ImportError as e:
            self.skipTest(f"polars not available: {e}")
        self._assert_records_valid(process_single_game(SAMPLE_GAME_INPUT))

    def test_ray_module_loads(self):
        # The actual @ray.remote functions need a live Ray runtime to call .remote(),
        # which is too heavy for a unit test. We just verify the module imports and
        # exposes the expected remote functions.
        try:
            import lichess_etl_1_extraction_ray as ray_mod
        except ImportError as e:
            self.skipTest(f"ray not available: {e}")
        self.assertTrue(hasattr(ray_mod, "process_single_game_remote"))
        self.assertTrue(hasattr(ray_mod, "process_batch_remote"))
        self.assertTrue(hasattr(ray_mod, "process_lichess_ray"))

    def test_auto_update_worker(self):
        from lichess_auto_update import _process_game
        self._assert_records_valid(_process_game(SAMPLE_GAME_INPUT))

    def test_benchmark_worker(self):
        from benchmark_etl1 import _parse_game
        self._assert_records_valid(_parse_game(SAMPLE_GAME_INPUT))

    def test_invalid_pgn_returns_empty_list(self):
        from lichess_etl_1_extraction_parallel import process_single_game
        bad_input = ("not a real pgn @@@", "Event", 1500, 1500)
        # process_single_game swallows exceptions and returns []
        self.assertEqual(process_single_game(bad_input), [])


# =========================================================
# lichess_auto_update.py — month tracking + HTML scraping
# =========================================================
class TestAutoUpdateTracking(unittest.TestCase):
    def setUp(self):
        import lichess_auto_update
        self.module = lichess_auto_update
        self.tmpdir = tempfile.mkdtemp()
        self.tracking_path = os.path.join(self.tmpdir, "processed_months.json")
        self.patcher = mock.patch.object(self.module, "TRACKING_FILE", self.tracking_path)
        self.patcher.start()

    def tearDown(self):
        self.patcher.stop()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_load_returns_empty_set_when_file_missing(self):
        self.assertEqual(self.module.load_processed_months(), set())

    def test_save_then_load_roundtrip(self):
        self.module.save_processed_month("2024-01")
        self.module.save_processed_month("2024-02")
        self.assertEqual(
            self.module.load_processed_months(),
            {"2024-01", "2024-02"},
        )
        # File should be valid JSON sorted alphabetically.
        with open(self.tracking_path) as f:
            self.assertEqual(json.load(f), ["2024-01", "2024-02"])

    def test_save_is_idempotent(self):
        self.module.save_processed_month("2024-01")
        self.module.save_processed_month("2024-01")
        self.assertEqual(self.module.load_processed_months(), {"2024-01"})


class TestAutoUpdateMonthDiscovery(unittest.TestCase):
    def test_fetch_available_months_parses_html(self):
        sample_html = """
        <html><body>
        <a href="standard/lichess_db_standard_rated_2024-01.pgn.zst">jan</a>
        <a href="standard/lichess_db_standard_rated_2024-02.pgn.zst">feb</a>
        <a href="standard/lichess_db_standard_rated_2023-12.pgn.zst">dec</a>
        <a href="other_unrelated_file.zip">noise</a>
        </body></html>
        """
        fake_response = mock.Mock(text=sample_html)
        fake_response.raise_for_status = mock.Mock()

        with mock.patch("lichess_auto_update.requests.get", return_value=fake_response):
            from lichess_auto_update import fetch_available_months
            months = fetch_available_months()

        # Returned tuples are (year_month, url), sorted alphabetically by year_month.
        labels = [m[0] for m in months]
        self.assertEqual(labels, ["2023-12", "2024-01", "2024-02"])
        self.assertTrue(
            months[0][1].endswith("standard/lichess_db_standard_rated_2023-12.pgn.zst")
        )


# =========================================================
# benchmark_etl1.py — game loader filtering
# =========================================================
class TestBenchmarkLoader(unittest.TestCase):
    def test_load_games_filters_bullet_and_low_elo(self):
        from benchmark_etl1 import load_games

        sample_pgn = (
            '[Event "Rated Blitz game"]\n'
            '[WhiteElo "1500"]\n[BlackElo "1450"]\n[Termination "Normal"]\n\n'
            '1. e4 e5 1-0\n\n'
            # Bullet — excluded
            '[Event "Rated Bullet game"]\n'
            '[WhiteElo "2000"]\n[BlackElo "2000"]\n[Termination "Normal"]\n\n'
            '1. d4 d5 1-0\n\n'
            # Low ELO — excluded (both <= 600)
            '[Event "Rated Blitz game"]\n'
            '[WhiteElo "500"]\n[BlackElo "500"]\n[Termination "Normal"]\n\n'
            '1. c4 c5 1-0\n\n'
            # Abandoned — excluded
            '[Event "Rated Blitz game"]\n'
            '[WhiteElo "1800"]\n[BlackElo "1850"]\n[Termination "Abandoned"]\n\n'
            '1. Nf3 Nf6 1-0\n\n'
            # Valid
            '[Event "Rated Rapid game"]\n'
            '[WhiteElo "1700"]\n[BlackElo "1750"]\n[Termination "Normal"]\n\n'
            '1. d4 Nf6 1-0\n'
        )

        with tempfile.NamedTemporaryFile(
            "w", suffix=".pgn", delete=False, encoding="utf-8"
        ) as f:
            f.write(sample_pgn)
            tmp_path = f.name

        try:
            games = load_games(tmp_path, max_games=10)
        finally:
            os.unlink(tmp_path)

        self.assertEqual(len(games), 2)
        events = [g[1] for g in games]
        self.assertNotIn("Rated Bullet game", events)
        for _, _, w_elo, b_elo in games:
            self.assertGreater(w_elo, 600)
            self.assertGreater(b_elo, 600)

    def test_load_games_respects_max_games_cap(self):
        from benchmark_etl1 import load_games

        # Build a PGN with 5 valid games.
        game_block = (
            '[Event "Rated Blitz game"]\n'
            '[WhiteElo "1500"]\n[BlackElo "1500"]\n[Termination "Normal"]\n\n'
            '1. e4 e5 1-0\n\n'
        )
        with tempfile.NamedTemporaryFile(
            "w", suffix=".pgn", delete=False, encoding="utf-8"
        ) as f:
            f.write(game_block * 5)
            tmp_path = f.name

        try:
            games = load_games(tmp_path, max_games=3)
        finally:
            os.unlink(tmp_path)

        self.assertEqual(len(games), 3)


# =========================================================
# Top-level scripts that have side effects on import (run DuckDB queries
# at module load). Just verify they parse as valid Python.
# =========================================================
class TestScriptSyntax(unittest.TestCase):
    def _assert_valid_python(self, filename):
        path = os.path.join(PROJECT_ROOT, filename)
        with open(path, encoding="utf-8") as f:
            source = f.read()
        compile(source, path, "exec")

    def test_lichess_etl_2_grouping_parses(self):
        self._assert_valid_python("lichess_etl_2_grouping.py")

    def test_lichess_etl_3_final_merging_parses(self):
        self._assert_valid_python("lichess_etl_3_final_merging.py")


if __name__ == "__main__":
    unittest.main(verbosity=2)
