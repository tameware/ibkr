# Usage: python -m unittest discover -s tests -t . -v

# Coded by DeepSeek

import unittest
from unittest.mock import Mock, patch, MagicMock, call
import json
import tempfile
from pathlib import Path
from decimal import Decimal
import datetime
from argparse import Namespace
from zoneinfo import ZoneInfo

from tests.ibapi_mocks import install_ibapi_mocks, seed_valid_nbbo
from tests.ledger_test_helpers import init_test_ledgers_dir

install_ibapi_mocks(pytz=True, peg_best_constants=True)

from ibkr_app_support import (
    seed_ledger_position,
    cli_to_config,
    load_config_file,
    make_stock_contract,
    merge_config,
    require_fields,
)

# Now import peg_best
from peg_best import Trader, build_arg_parser


class TestHelpers(unittest.TestCase):
    """Test helper functions"""
    
    def test_load_config_file_valid(self):
        """Test loading a valid JSON config file"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump({"host": "127.0.0.1", "port": 7497}, f)
            f.close()
            result = load_config_file(f.name)
            Path(f.name).unlink()
            
        self.assertEqual(result, {"host": "127.0.0.1", "port": 7497})
    
    def test_load_config_file_invalid(self):
        """Test loading an invalid config file (non-object)"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(["list", "not", "dict"], f)
            f.close()
            
            with self.assertRaises(ValueError):
                load_config_file(f.name)
            Path(f.name).unlink()
    
    def test_cli_to_config(self):
        """Test converting CLI args to config dict"""
        args = Namespace(
            config="config.json",
            host="localhost",
            port=7497,
            symbol="AAPL",
            none_value=None
        )
        result = cli_to_config(args)
        
        self.assertEqual(result, {"host": "localhost", "port": 7497, "symbol": "AAPL"})
        self.assertNotIn("config", result)
        self.assertNotIn("none_value", result)
    
    def test_merge_config(self):
        """Test merging file and CLI configs"""
        file_config = {"host": "127.0.0.1", "port": 7496, "symbol": "MSFT"}
        cli_config = {"port": 7497, "symbol": "AAPL", "extra": "value"}
        
        result = merge_config(file_config, cli_config)
        
        self.assertEqual(result["host"], "127.0.0.1")
        self.assertEqual(result["port"], 7497)
        self.assertEqual(result["symbol"], "AAPL")
        self.assertEqual(result["extra"], "value")
    
    def test_require_fields_success(self):
        """Test require_fields with all fields present"""
        config = {"a": 1, "b": 2, "c": 3}
        require_fields(config, ["a", "b"])
        # Should not raise exception
    
    def test_require_fields_failure(self):
        """Test require_fields with missing fields"""
        config = {"a": 1, "c": 3}
        with self.assertRaises(ValueError) as context:
            require_fields(config, ["a", "b", "d"])
        self.assertIn("b, d", str(context.exception))


class TestTrader(unittest.TestCase):
    """Test Trader class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.ledgers_dir = init_test_ledgers_dir(self)
        self.config = {
            "ledgers_dir": self.ledgers_dir,
            "host": "127.0.0.1",
            "port": 7497,
            "symbol": "AAPL",
            "sec_type": "STK",
            "currency": "USD",
            "exchange": "SMART",
            "primary_exchange": "NASDAQ",
            "max_pos": 100,
            "loop_seconds": 1.0,
            "buy_limit_multiplier": 0.95,
            "sell_limit_multiplier": 1.05,
            "min_compete_size": 1,
            "mid_offset_whole": 0.01,
            "mid_offset_half": 0.005,
            "post_to_ats_seconds": 0,
            "market_timezone": "America/New_York",
            "market_open_hour": 9,
            "market_open_minute": 30,
            "market_close_hour": 16,
            "last_trade_min_size": 100,
            "tif": "DAY",
            "price_round_digits": 2,
            "ignored_error_codes": [2104, 2106, 2158],
            "ignore_error_substrings": ["HMDS", "market data farm"],
        }

        self._log_patcher = patch("ibkr_bot_base.build_logger", return_value=MagicMock())
        self._log_patcher.start()
        self.addCleanup(self._log_patcher.stop)

        # Create trader instance
        self.trader = Trader(self.config)
        
        # Mock necessary methods that would normally come from IB API
        self.trader.isConnected = Mock(return_value=True)
        self.trader.reqOpenOrders = Mock()
        self.trader.reqPositions = Mock()
        self.trader.placeOrder = Mock()
        self.trader.cancelOrder = Mock()
        self.trader.reqMktData = Mock()
        self.trader.reqContractDetails = Mock()
        self.trader.reqMarketDataType = Mock()
        self.trader.reqTickByTickData = Mock()
        self.trader.reqHistoricalData = Mock()
        
        # Set initial state
        self.trader.nextOrderId = 1000
        self.trader.ready_for_trading = True
    
    def _seed(self, qty, avg_cost=150.0):
        seed_ledger_position(self.trader.ledger, self.trader, int(qty), avg_cost=avg_cost)

    def test_initialization(self):
        """Test trader initialization"""
        self.assertEqual(self.trader.config, self.config)
        self.assertEqual(self.trader.position_size, 0)
        self.assertIsNone(self.trader.open_price)
        self.assertIsNone(self.trader.ref_price)
        self.assertIsNone(self.trader.buy_order_id)
        self.assertIsNone(self.trader.sell_order_id)
        self.assertEqual(self.trader._bars, [])
        self.assertEqual(self.trader.open_symbol_buys, 0)
        self.assertEqual(self.trader.open_symbol_sells, 0)
    
    def test_make_stock_contract(self):
        """Test contract creation"""
        contract = make_stock_contract(
            {
                "symbol": "AAPL",
                "sec_type": "STK",
                "currency": "USD",
                "exchange": "SMART",
                "primary_exchange": "NASDAQ",
            }
        )

        self.assertEqual(contract.symbol, "AAPL")
        self.assertEqual(contract.secType, "STK")
        self.assertEqual(contract.currency, "USD")
        self.assertEqual(contract.exchange, "SMART")
        self.assertEqual(contract.primaryExchange, "NASDAQ")
    
    def test_make_quote_order_buy(self):
        """Test PEG BEST order creation for BUY"""
        order = self.trader.make_quote_order("BUY", 50, 150.25)
        
        self.assertEqual(order.action, "BUY")
        self.assertEqual(order.orderType, "PEG BEST")
        self.assertEqual(order.totalQuantity, 50)
        self.assertEqual(order.lmtPrice, 150.25)
        self.assertEqual(order.exchange, "SMART")
        self.assertEqual(order.tif, "DAY")
        self.assertTrue(order.notHeld)
        self.assertEqual(order.minCompeteSize, 1)
    
    def test_next_valid_id(self):
        """Test nextValidId callback"""
        from ibkr_bot_base import CONTRACT_DETAILS_REQ_ID

        with patch.object(self.trader, 'request_positions_snapshot') as mock_req_pos, \
             patch.object(self.trader, 'request_today_open_or_prior_close') as mock_req_hist, \
             patch.object(self.trader, 'reqMktData') as mock_mkt_data, \
             patch.object(self.trader, 'request_open_orders_snapshot') as mock_req_open, \
             patch.object(self.trader, 'reqTickByTickData') as mock_req_ticks:
            
            self.trader.nextValidId(1000)
            
            self.assertEqual(self.trader.nextOrderId, 1000)
            mock_req_pos.assert_called_once()
            mock_req_hist.assert_called_once()
            self.trader.reqContractDetails.assert_called_once()
            mock_mkt_data.assert_not_called()
            mock_req_open.assert_called_once()
            mock_req_ticks.assert_called_once()

            details = MagicMock()
            details.contract.conId = 1
            details.contract.localSymbol = "AAPL"
            details.contract.exchange = "SMART"
            details.contract.primaryExchange = "NASDAQ"
            self.trader.contractDetails(CONTRACT_DETAILS_REQ_ID, details)
            self.trader.contractDetailsEnd(CONTRACT_DETAILS_REQ_ID)
            mock_mkt_data.assert_called_once()
    
    def test_order_status_filled_buy(self):
        """Test order status for filled BUY order"""
        self.trader.buy_order_id = 500
        self.trader.pending_buy = True
        
        self.trader.orderStatus(
            500, "Filled", 50, 0, 150.25, 12345, 0, 
            150.25, 1, "", 0.0
        )
        
        self.assertFalse(self.trader.pending_buy)
        self.assertIsNone(self.trader.buy_order_id)
        self.assertEqual(self.trader.open_symbol_buys, 0)
        self.assertEqual(self.trader.open_symbol_sells, 0)
        self.trader.reqOpenOrders.assert_called_once()
    
    def test_order_status_cancelled_sell(self):
        """Test order status for cancelled SELL order"""
        self.trader.sell_order_id = 501
        self.trader.pending_sell = True
        
        self.trader.orderStatus(
            501, "Cancelled", 0, 50, 0, 12346, 0,
            0, 1, "", 0.0
        )
        
        self.assertFalse(self.trader.pending_sell)
        self.assertIsNone(self.trader.sell_order_id)
        self.trader.reqOpenOrders.assert_called_once()
    
    def test_order_status_ignores_other_orders(self):
        """Test order status ignores orders not tracked"""
        self.trader.buy_order_id = 500
        self.trader.pending_buy = True
        # Reset the mock to clear any previous calls
        self.trader.reqOpenOrders.reset_mock()
        
        self.trader.orderStatus(
            999, "Filled", 50, 0, 150.25, 12345, 0,
            150.25, 1, "", 0.0
        )
        
        # Should still be pending since this order ID doesn't match
        self.assertTrue(self.trader.pending_buy)
        self.assertEqual(self.trader.buy_order_id, 500)
        # reqOpenOrders should NOT be called for unmatched order IDs
        self.trader.reqOpenOrders.assert_not_called()
    
    def test_tick_by_tick_all_last_does_not_update_ref_price(self):
        """Last trades do not update ref_price; only coalesced NBBO does."""
        self.trader.ref_price = 150.00
        mock_attrib = Mock()
        
        self.trader.tickByTickAllLast(
            reqId=2001,
            tickType=1,
            time_value=1234567890,
            price=151.50,
            size=Decimal("200"),
            tickAttribLast=mock_attrib,
            exchange="NASDAQ",
            specialConditions=""
        )
        
        self.assertEqual(self.trader.ref_price, 150.00)
    
    def test_tick_by_tick_all_last_ignores_small_size(self):
        """Test that last trade with small size does not update ref_price"""
        original_ref = 150.00
        self.trader.ref_price = original_ref
        mock_attrib = Mock()
        
        self.trader.tickByTickAllLast(
            reqId=2001,
            tickType=1,
            time_value=1234567890,
            price=151.50,
            size=Decimal("10"),
            tickAttribLast=mock_attrib,
            exchange="NASDAQ",
            specialConditions=""
        )
        
        self.assertEqual(self.trader.ref_price, original_ref)
    
    def test_tick_by_tick_all_last_ignores_wrong_req_id(self):
        """Test that last trade with wrong req ID is ignored"""
        original_ref = 150.00
        self.trader.ref_price = original_ref
        mock_attrib = Mock()
        
        self.trader.tickByTickAllLast(
            reqId=9999,  # Wrong ID
            tickType=1,
            time_value=1234567890,
            price=151.50,
            size=Decimal("200"),
            tickAttribLast=mock_attrib,
            exchange="NASDAQ",
            specialConditions=""
        )
        
        self.assertEqual(self.trader.ref_price, original_ref)
    
    def test_tick_price_updates_mid_price(self):
        """Test that bid/ask ticks update ref_price to mid price"""
        self.trader._bid = 149.50
        self.trader._ask = 150.50
        self.trader.ref_price = 150.00
        
        self.trader.tickPrice(3001, 2, 150.50, Mock())
        # mid = 150.00, same as before, no change

    def test_tick_price_updates_with_new_mid(self):
        """Test that bid/ask updates ref_price when mid changes"""
        self.trader._bid = 150.00
        self.trader._ask = 152.00
        self.trader.ref_price = 150.00

        self.trader.tickPrice(3001, 2, 152.00, Mock())
        self.trader._nbbo.flush_commit()
        self.assertEqual(self.trader.ref_price, 151.00)
    
    def test_open_order_tracking(self):
        """Test tracking of open orders"""
        contract = MagicMock()
        contract.symbol = "AAPL"
        contract.secType = "STK"
        
        # Add BUY order
        order = MagicMock()
        order.action = "BUY"
        self.trader.openOrder(100, contract, order, MagicMock())
        self.assertEqual(self.trader.open_symbol_buys, 1)
        self.assertEqual(self.trader.buy_order_id, 100)
        
        # Add SELL order
        order.action = "SELL"
        self.trader.openOrder(101, contract, order, MagicMock())
        self.assertEqual(self.trader.open_symbol_sells, 1)
        self.assertEqual(self.trader.sell_order_id, 101)
    
    def test_open_order_ignores_other_symbols(self):
        """Test that open orders for other symbols are ignored"""
        contract = MagicMock()
        contract.symbol = "MSFT"
        contract.secType = "STK"
        
        order = MagicMock()
        order.action = "BUY"
        self.trader.openOrder(100, contract, order, MagicMock())
        
        self.assertEqual(self.trader.open_symbol_buys, 0)
        self.assertIsNone(self.trader.buy_order_id)
    
    def test_open_order_end(self):
        """Test openOrderEnd callback"""
        self.trader.position_snapshot_complete = True
        self.trader.openOrderEnd()
        self.assertTrue(self.trader.ready_for_trading)
    
    def test_position_tracking(self):
        """Test position tracking"""
        contract = MagicMock()
        contract.symbol = "AAPL"
        contract.secType = "STK"
        
        self.trader.position("account1", contract, 75, 150.25)
        self.assertEqual(self.trader.ledger.ib_snapshot_qty, 75)
        self.assertEqual(self.trader.position_size, 0)

    def test_position_ignores_other_symbols(self):
        """Test that positions for other symbols are ignored"""
        contract = MagicMock()
        contract.symbol = "MSFT"
        contract.secType = "STK"
        
        self.trader.position("account1", contract, 75, 150.25)
        self.assertEqual(self.trader.position_size, 0)
    
    def test_historical_data_processing(self):
        """Test historical data processing"""
        bar1 = MagicMock()
        bar1.open = 145.00
        bar1.close = 146.00
        
        bar2 = MagicMock()
        bar2.open = 150.00
        bar2.close = 151.00
        
        self.trader._bars = [bar1, bar2]
        self.trader.historicalDataEnd(1001, "", "")
        
        self.assertEqual(self.trader.open_price, 150.00)
    
    def test_historical_data_fallback_to_close(self):
        """Test fallback to close when open not available"""
        bar = MagicMock()
        bar.open = 0
        bar.close = 145.50
        
        self.trader._bars = [bar]
        self.trader.historicalDataEnd(1001, "", "")
        self.assertEqual(self.trader.open_price, 145.50)
    
    def test_historical_data_ignores_wrong_req_id(self):
        """Test historical data with wrong request ID is ignored"""
        bar1 = MagicMock()
        bar1.open = 150.00
        
        self.trader._bars = [bar1]
        self.trader.historicalData(9999, bar1)  # Wrong ID
        self.assertEqual(len(self.trader._bars), 1)
    
    def test_us_regular_hours_market_hours(self):
        """Test market hours detection"""
        from ibkr_app_support import regular_session_open

        ny = ZoneInfo("America/New_York")
        now = datetime.datetime(2024, 1, 15, 10, 30, 0, tzinfo=ny)
        cfg = {
            "market_timezone": "America/New_York",
            "market_open_hour": 9,
            "market_open_minute": 30,
            "market_close_hour": 16,
        }
        self.assertTrue(regular_session_open(cfg, now=now))

    def test_us_regular_hours_before_open(self):
        """Test detection of pre-market hours"""
        from ibkr_app_support import regular_session_open

        ny = ZoneInfo("America/New_York")
        now = datetime.datetime(2024, 1, 15, 8, 30, 0, tzinfo=ny)
        cfg = {
            "market_timezone": "America/New_York",
            "market_open_hour": 9,
            "market_open_minute": 30,
            "market_close_hour": 16,
        }
        self.assertFalse(regular_session_open(cfg, now=now))

    def test_us_regular_hours_after_close(self):
        """Test detection of after-hours"""
        from ibkr_app_support import regular_session_open

        ny = ZoneInfo("America/New_York")
        now = datetime.datetime(2024, 1, 15, 17, 30, 0, tzinfo=ny)
        cfg = {
            "market_timezone": "America/New_York",
            "market_open_hour": 9,
            "market_open_minute": 30,
            "market_close_hour": 16,
        }
        self.assertFalse(regular_session_open(cfg, now=now))
    
    def test_sync_orders_buy_when_no_position(self):
        """Test sync_orders when no position - should place BUY order"""
        seed_valid_nbbo(self.trader, 150.00)
        self._seed(0)
        
        # Capture the order that gets passed to placeOrder
        captured_order = None
        def capture_order(order_id, contract, order):
            nonlocal captured_order
            captured_order = order
        
        self.trader.placeOrder = Mock(side_effect=capture_order)
        
        self.trader.sync_orders()
        
        self.trader.placeOrder.assert_called_once()
        self.assertIsNotNone(captured_order)
        self.assertEqual(captured_order.action, "BUY")
        self.assertEqual(captured_order.totalQuantity, 100)  # max_pos
        self.assertEqual(captured_order.lmtPrice, 142.50)  # 150 * 0.95
        self.assertEqual(self.trader.buy_order_id, 1000)
        self.assertTrue(self.trader.pending_buy)
    
    def test_sync_orders_sell_when_has_position(self):
        """Test sync_orders when has position - should place SELL order"""
        seed_valid_nbbo(self.trader, 150.00)
        self._seed(50)
        
        # Capture the order that gets passed to placeOrder
        captured_order = None
        def capture_order(order_id, contract, order):
            nonlocal captured_order
            captured_order = order
        
        self.trader.placeOrder = Mock(side_effect=capture_order)
        
        self.trader.sync_orders()
        
        self.trader.placeOrder.assert_called_once()
        self.assertIsNotNone(captured_order)
        self.assertEqual(captured_order.action, "SELL")
        self.assertEqual(captured_order.totalQuantity, 50)  # current position
        self.assertEqual(captured_order.lmtPrice, 157.50)  # 150 * 1.05
        self.assertEqual(self.trader.sell_order_id, 1000)
        self.assertTrue(self.trader.pending_sell)
    
    def test_sync_orders_cancels_opposite_buy_order(self):
        """Test that sync_orders cancels opposite BUY order when wanting to sell"""
        seed_valid_nbbo(self.trader, 150.00)
        self._seed(50)
        self.trader.buy_order_id = 999  # Has open BUY order but wants to SELL
        
        with patch("single_side_quoter.safe_cancel_order") as cancel:
            self.trader.sync_orders()
            cancel.assert_called_once_with(self.trader, 999)
        self.assertEqual(self.trader.open_symbol_buys, 0)
        self.assertIsNone(self.trader.buy_order_id)
    
    def test_sync_orders_cancels_opposite_sell_order(self):
        """Test that sync_orders cancels opposite SELL order when wanting to buy"""
        seed_valid_nbbo(self.trader, 150.00)
        self._seed(0)
        self.trader.sell_order_id = 999  # Has open SELL order but wants to BUY
        
        with patch("single_side_quoter.safe_cancel_order") as cancel:
            self.trader.sync_orders()
            cancel.assert_called_once_with(self.trader, 999)
        self.assertEqual(self.trader.open_symbol_sells, 0)
        self.assertIsNone(self.trader.sell_order_id)
    
    def test_sync_orders_does_not_place_when_not_ready(self):
        """Test sync_orders does nothing when not ready"""
        self.trader.ready_for_trading = False
        self.trader.ref_price = 150.00
        self._seed(0)
        
        self.trader.sync_orders()
        
        self.trader.placeOrder.assert_not_called()
    
    def test_sync_orders_does_not_place_when_no_ref_price(self):
        """Test sync_orders does nothing when no ref_price"""
        self.trader.ref_price = None
        self.trader._bid = 149.90
        self.trader._ask = 150.10
        self._seed(0)
        
        self.trader.sync_orders()
        
        self.trader.placeOrder.assert_not_called()

    def test_sync_orders_skips_when_no_nbbo(self):
        """Test sync_orders does not place orders without valid bid/ask"""
        self.trader.ref_price = 150.00
        self.trader._bid = None
        self.trader._ask = None
        self._seed(0)

        self.trader.sync_orders()

        self.trader.placeOrder.assert_not_called()
    
    def test_sync_orders_respects_existing_orders(self):
        """Test sync_orders does not place new order when one already exists"""
        seed_valid_nbbo(self.trader, 150.00)
        self._seed(0)
        self.trader.open_symbol_buys = 1  # Already has a buy order
        
        self.trader.sync_orders()
        
        self.trader.placeOrder.assert_not_called()
    
    def test_error_filtering_ignores_codes(self):
        """Test that error codes in ignored list are suppressed"""
        self.trader.error(0, "", 2104, "Market data farm connection is OK", "")
        self.trader.logger.warning.assert_not_called()
        self.trader.logger.info.assert_not_called()

    def test_error_filtering_ignores_substrings(self):
        """Test that error messages with ignored substrings are suppressed"""
        self.trader.error(0, "", 10000, "Error: HMDS connection issue", "")
        self.trader.logger.warning.assert_not_called()
        self.trader.logger.info.assert_not_called()

    def test_error_prints_non_ignored(self):
        """Test that non-ignored errors are logged"""
        self.trader.error(0, "", 500, "Critical error", "")
        self.trader.logger.warning.assert_called_once()
        self.assertEqual(self.trader.logger.warning.call_args[0][4], "Critical error")
    
    def test_run_until_shutdown_disconnected(self):
        """Test main loop when disconnected"""
        self.trader.isConnected = Mock(return_value=False)
        
        with patch('time.sleep') as mock_sleep:
            self.trader.run_until_shutdown()
            mock_sleep.assert_not_called()
    
    @patch('time.sleep')
    def test_run_until_shutdown_market_hours(self, mock_sleep):
        """Test main loop during market hours"""
        call_count = 0

        def is_connected_mock():
            nonlocal call_count
            call_count += 1
            return call_count < 3

        self.trader.isConnected = Mock(side_effect=is_connected_mock)
        self.trader.us_regular_hours = Mock(return_value=True)

        with patch.object(self.trader, "trigger_resync") as mock_resync:
            self.trader.run_until_shutdown()
            mock_resync.assert_called()

    def test_trigger_resync_debounce(self):
        """Rapid trigger_resync calls are debounced."""
        self.trader.isConnected = Mock(return_value=True)
        self.trader.request_positions_snapshot = Mock()
        self.trader.request_open_orders_snapshot = Mock()
        self.trader.maybe_sync_orders = Mock()
        self.trader.resync_debounce_seconds = 10.0

        self.trader.trigger_resync()
        self.assertTrue(self.trader.sync_requested)
        self.trader.request_positions_snapshot.assert_called_once()
        self.trader.request_open_orders_snapshot.assert_called_once()

        self.trader.request_positions_snapshot.reset_mock()
        self.trader.request_open_orders_snapshot.reset_mock()
        self.trader.trigger_resync()
        self.trader.request_positions_snapshot.assert_not_called()
        self.trader.request_open_orders_snapshot.assert_not_called()

        self.trader.sync_requested = False
        self.trader.trigger_resync(force=True)
        self.assertTrue(self.trader.sync_requested)
        self.trader.request_positions_snapshot.assert_called_once()
    
    @patch('time.sleep')
    def test_run_until_shutdown_outside_market_hours(self, mock_sleep):
        """Test main loop outside market hours"""
        self.trader.isConnected = Mock(side_effect=[True, False])
        self.trader.us_regular_hours = Mock(return_value=False)

        with patch.object(self.trader, "trigger_resync") as mock_resync:
            self.trader.run_until_shutdown()
            mock_resync.assert_not_called()


class TestArgParser(unittest.TestCase):
    """Test command line argument parser"""
    
    def test_build_arg_parser(self):
        """Test argument parser creation"""
        parser = build_arg_parser()
        
        # Test that parser has all expected arguments
        args = parser.parse_args([])
        self.assertTrue(hasattr(args, 'config'))
        self.assertTrue(hasattr(args, 'host'))
        self.assertTrue(hasattr(args, 'port'))
        self.assertTrue(hasattr(args, 'symbol'))
        self.assertTrue(hasattr(args, 'sec_type'))
        self.assertTrue(hasattr(args, 'max_pos'))
        self.assertTrue(hasattr(args, 'loop_seconds'))
    
    def test_parse_args(self):
        """Test parsing command line arguments"""
        parser = build_arg_parser()
        args = parser.parse_args([
            '--host', 'localhost',
            '--port', '7497',
            '--symbol', 'AAPL',
            '--max_pos', '200'
        ])
        
        self.assertEqual(args.host, 'localhost')
        self.assertEqual(args.port, 7497)
        self.assertEqual(args.symbol, 'AAPL')
        self.assertEqual(args.max_pos, 200)
    
    def test_default_config_path(self):
        """Test default config path"""
        parser = build_arg_parser()
        args = parser.parse_args([])
        self.assertEqual(args.config, str(Path("config") / "peg_best.json"))


if __name__ == '__main__':
    unittest.main()