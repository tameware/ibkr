# Usage python -m unittest test_peg_mid.py -v

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
import sys
import time

# Create mock classes for IB API before importing peg_mid
class MockEWrapper:
    pass

class MockEClient:
    def __init__(self, wrapper):
        pass

class MockContract:
    def __init__(self):
        self.symbol = ""
        self.secType = ""
        self.currency = ""
        self.exchange = ""
        self.primaryExch = ""

class MockOrder:
    def __init__(self):
        self.action = ""
        self.orderType = ""
        self.totalQuantity = 0
        self.lmtPrice = 0.0
        self.auxPrice = 0.0
        self.midOffsetAtWhole = 0.0
        self.midOffsetAtHalf = 0.0
        self.notHeld = False
        self.exchange = ""
        self.tif = ""

# Set up the module mocks
mock_ibapi = MagicMock()
mock_ibapi.wrapper.EWrapper = MockEWrapper
mock_ibapi.client.EClient = MockEClient
mock_ibapi.contract.Contract = MockContract
mock_ibapi.order.Order = MockOrder

sys.modules['ibapi'] = mock_ibapi
sys.modules['ibapi.client'] = mock_ibapi.client
sys.modules['ibapi.wrapper'] = mock_ibapi.wrapper
sys.modules['ibapi.common'] = MagicMock()
sys.modules['ibapi.contract'] = mock_ibapi.contract
sys.modules['ibapi.order'] = mock_ibapi.order
sys.modules['ibapi.ticktype'] = MagicMock()

from ibkr_app_support import (
    seed_ledger_position,
    cli_to_config,
    load_config_file,
    make_stock_contract,
    merge_config,
    require_fields,
)

# Now import peg_mid
from peg_mid import Trader, tprint, build_arg_parser


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
        self._ledger_tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._ledger_tmp.cleanup)
        self.config = {
            "ledgers_dir": self._ledger_tmp.name,
            "host": "127.0.0.1",
            "port": 7497,
            "symbol": "AAPL",
            "sec_type": "STK",
            "currency": "USD",
            "exchange": "SMART",
            "primary_exchange": "NASDAQ",
            "max_pos": 100,
            "loop_seconds": 1.0,
            "mid_delta": 0.01,
            "buy_delta": 0.05,
            "sell_delta": 0.05,
            "market_timezone": "America/New_York",
            "market_open_hour": 9,
            "market_open_minute": 30,
            "market_close_hour": 16,
            "last_trade_min_size": 100,
            "tif": "DAY",
            "price_round_digits": 2,
            "ignored_error_codes": [2104, 2106, 2158],
            "ignore_error_substrings": ["HMDS", "market data farm"],
            "resync_debounce_seconds": 0.35,
            "peg_mid_offset_whole": 0.01,
            "peg_mid_offset_half": 0.015,
        }
        
        # Create trader instance
        self.trader = Trader(self.config)
        
        # Mock necessary methods that would normally come from IB API
        self.trader.isConnected = Mock(return_value=True)
        self.trader.serverVersion = Mock(return_value=157)
        self.trader.reqOpenOrders = Mock()
        self.trader.reqPositions = Mock()
        self.trader.placeOrder = Mock()
        self.trader.cancelOrder = Mock()
        self.trader.reqMktData = Mock()
        self.trader.reqTickByTickData = Mock()
        self.trader.reqHistoricalData = Mock()
        
        # Set initial state - CRITICAL: Set ready_for_trading to True for sync_orders
        self.trader.nextOrderId = 1000
        self.trader.ready_for_trading = True  # This is key!
        self.trader.ref_price = 150.00
        self.trader._bid = 150.00
        self.trader._ask = 150.02
        
        # Reset order tracking
        self.trader.buy_order_id = None
        self.trader.sell_order_id = None
        self.trader.open_symbol_buys = 0
        self.trader.open_symbol_sells = 0
        self.trader.pending_buy = False
        self.trader.pending_sell = False
        self._seed(0)
        self.trader.remaining_by_order = {}
        self.trader.last_resync_request_ts = 0.0
        
        # Mock request methods (but NOT sync_orders - we want the real one)
        self.trader.request_open_orders_snapshot = Mock()
        self.trader.request_positions_snapshot = Mock()
        # Do NOT mock sync_orders - we want to test the real method
    
    def _seed(self, qty, avg_cost=150.0):
        seed_ledger_position(self.trader.ledger, self.trader, int(qty), avg_cost=avg_cost)

    def test_initialization(self):
        """Test trader initialization"""
        self.assertEqual(self.trader.config, self.config)
        self.assertEqual(self.trader.position_size, 0)
        self.assertIsNone(self.trader.open_price)
        self.assertIsNone(self.trader.buy_order_id)
        self.assertIsNone(self.trader.sell_order_id)
        self.assertEqual(self.trader._bars, [])
        self.assertEqual(self.trader.open_symbol_buys, 0)
        self.assertEqual(self.trader.open_symbol_sells, 0)
        self.assertFalse(self.trader.pending_buy)
        self.assertFalse(self.trader.pending_sell)
        self.assertFalse(self.trader.sync_requested)
        self.assertEqual(self.trader.resync_debounce_seconds, 0.35)
        self.assertEqual(self.trader.config["peg_mid_offset_whole"], 0.01)
        self.assertEqual(self.trader.config["peg_mid_offset_half"], 0.015)
    
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
    
    def test_midpoint_is_half_penny(self):
        """Test half-penny midpoint detection"""
        # Test with no bid/ask
        self.trader._bid = None
        self.trader._ask = None
        self.assertFalse(self.trader.midpoint_is_half_penny())
        
        # Test with whole penny midpoint
        self.trader._bid = 150.00
        self.trader._ask = 150.02
        self.assertFalse(self.trader.midpoint_is_half_penny())
        
        # Test with half-penny midpoint
        self.trader._bid = 150.00
        self.trader._ask = 150.01
        self.assertTrue(self.trader.midpoint_is_half_penny())
    
    def test_current_peg_mid_offset_whole_penny(self):
        """Test PEG MID offset calculation for whole penny midpoint"""
        self.trader._bid = 150.00
        self.trader._ask = 150.02
        
        offset = self.trader.current_peg_mid_offset()
        self.assertEqual(offset, 0.01)
    
    def test_current_peg_mid_offset_half_penny(self):
        """Test PEG MID offset calculation for half-penny midpoint"""
        self.trader._bid = 150.00
        self.trader._ask = 150.01
        
        offset = self.trader.current_peg_mid_offset()
        self.assertEqual(offset, 0.015)
    
    def test_current_peg_mid_offset_fallback(self):
        """Test PEG MID offset fallback when half offset not configured"""
        self.trader._bid = 150.00
        self.trader._ask = 150.01
        # Temporarily remove half offset
        original_half = self.trader.config.get("peg_mid_offset_half")
        if "peg_mid_offset_half" in self.trader.config:
            del self.trader.config["peg_mid_offset_half"]
        
        offset = self.trader.current_peg_mid_offset()
        # Should fall back to whole offset + 0.005
        self.assertEqual(offset, 0.015)
        
        # Restore
        self.trader.config["peg_mid_offset_half"] = original_half
    
    def test_make_peg_mid_order_buy(self):
        """Test PEG MID order creation for BUY"""
        self.trader._bid = 150.00
        self.trader._ask = 150.02
        order = self.trader.make_peg_mid_order("BUY", 50, 150.25)
        
        self.assertEqual(order.action, "BUY")
        self.assertEqual(order.orderType, "PEG MID")
        self.assertEqual(order.totalQuantity, 50)
        self.assertEqual(order.lmtPrice, 150.25)
        self.assertEqual(order.midOffsetAtWhole, 0.01)
        self.assertTrue(order.notHeld)
        self.assertEqual(order.exchange, "SMART")
        self.assertEqual(order.tif, "DAY")
    
    def test_make_peg_mid_order_sell_half_penny(self):
        """Test PEG MID order creation for SELL with half-penny midpoint"""
        self.trader._bid = 150.00
        self.trader._ask = 150.01
        order = self.trader.make_peg_mid_order("SELL", 30, 155.75)
        
        self.assertEqual(order.action, "SELL")
        self.assertEqual(order.orderType, "PEG MID")
        self.assertEqual(order.totalQuantity, 30)
        self.assertEqual(order.lmtPrice, 155.75)
        self.assertEqual(order.midOffsetAtHalf, 0.015)
        self.assertTrue(order.notHeld)
    
    def test_next_valid_id(self):
        """Test nextValidId callback"""
        with patch.object(self.trader, 'request_positions_snapshot') as mock_req_pos, \
             patch.object(self.trader, 'request_today_open_or_prior_close') as mock_req_hist, \
             patch.object(self.trader, 'reqMktData') as mock_mkt_data, \
             patch.object(self.trader, 'request_open_orders_snapshot') as mock_req_open, \
             patch.object(self.trader, 'reqTickByTickData') as mock_req_ticks:
            
            self.trader.nextValidId(1000)
            
            self.assertEqual(self.trader.nextOrderId, 1000)
            mock_req_pos.assert_called_once()
            mock_req_hist.assert_called_once()
            mock_mkt_data.assert_called_once()
            mock_req_open.assert_called_once()
            mock_req_ticks.assert_called_once()
    
    def test_order_status_submitted_buy(self):
        """Test order status for submitted BUY order"""
        self.trader.buy_order_id = 500
        self.trader.pending_buy = True
        
        self.trader.orderStatus(
            500, "Submitted", 0, 50, 0, 12345, 0,
            0, 1, "", 0.0
        )
        
        self.assertFalse(self.trader.pending_buy)
    
    def test_order_status_filled_buy_with_resync(self):
        """Test order status for filled BUY order triggers resync"""
        self.trader.buy_order_id = 500
        self.trader.pending_buy = True
        self.trader.trigger_resync = Mock()
        
        self.trader.orderStatus(
            500, "Filled", 50, 0, 150.25, 12345, 0,
            150.25, 1, "", 0.0
        )
        
        self.assertFalse(self.trader.pending_buy)
        self.assertIsNone(self.trader.buy_order_id)
        self.trader.trigger_resync.assert_called_once()
    
    def test_order_status_partially_filled_triggers_resync(self):
        """Test partial fill triggers resync"""
        self.trader.buy_order_id = 500
        self.trader.trigger_resync = Mock()
        
        self.trader.orderStatus(
            500, "PartiallyFilled", 25, 25, 150.25, 12345, 0,
            150.25, 1, "", 0.0
        )
        
        self.trader.trigger_resync.assert_called_once()
    
    def test_order_status_tracks_remaining(self):
        """Test order status tracks remaining quantity"""
        self.trader.orderStatus(
            500, "PartiallyFilled", 25, 25, 150.25, 12345, 0,
            150.25, 1, "", 0.0
        )
        
        self.assertIn(500, self.trader.remaining_by_order)
        self.assertEqual(self.trader.remaining_by_order[500], 25)
        
        self.trader.orderStatus(
            500, "PartiallyFilled", 40, 10, 150.25, 12345, 0,
            150.25, 1, "", 0.0
        )
        
        self.assertEqual(self.trader.remaining_by_order[500], 10)
    
    def test_tick_by_tick_all_last_updates_ref_price(self):
        """Test that last trade updates ref_price when size meets threshold"""
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
        
        self.assertEqual(self.trader.ref_price, 151.50)
    
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
    
    def test_tick_price_updates_ref_price(self):
        """Test that bid/ask updates ref_price to mid price"""
        self.trader._bid = 150.00
        self.trader._ask = 152.00
        self.trader.ref_price = 150.00
        
        with patch('peg_mid.tprint') as mock_print:
            self.trader.tickPrice(3001, 2, 152.00, Mock())
            self.assertEqual(self.trader.ref_price, 151.00)
            mock_print.assert_called_once()
    
    def test_open_order_tracking(self):
        """Test tracking of open orders"""
        contract = MagicMock()
        contract.symbol = "AAPL"
        contract.secType = "STK"
        
        order = MagicMock()
        order.action = "BUY"
        self.trader.openOrder(100, contract, order, MagicMock())
        self.assertEqual(self.trader.open_symbol_buys, 1)
        self.assertEqual(self.trader.buy_order_id, 100)
        
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
        self.trader.open_orders_snapshot_complete = False
        self.trader.position_snapshot_complete = True
        
        self.trader.openOrderEnd()
        
        self.assertTrue(self.trader.open_orders_snapshot_complete)
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
    
    def test_position_end(self):
        """Test positionEnd callback"""
        self.trader.position_snapshot_complete = False
        self.trader.open_orders_snapshot_complete = True
        
        self.trader.positionEnd()
        
        self.assertTrue(self.trader.position_snapshot_complete)
        self.assertTrue(self.trader.ready_for_trading)
    
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
        with patch('peg_mid.tprint'):
            self.trader.historicalDataEnd(1001, "", "")
            self.assertEqual(self.trader.open_price, 145.50)
    
    def test_historical_data_sets_ref_price(self):
        """Test that historical data sets ref_price if None"""
        self.trader.ref_price = None
        bar = MagicMock()
        bar.open = 150.00
        bar.close = 151.00
        
        self.trader._bars = [bar]
        self.trader.historicalDataEnd(1001, "", "")
        
        self.assertEqual(self.trader.ref_price, 150.00)
    
    def test_historical_data_ignores_wrong_req_id(self):
        """Test historical data with wrong request ID is ignored"""
        bar1 = MagicMock()
        bar1.open = 150.00
        
        self.trader._bars = [bar1]
        self.trader.historicalData(9999, bar1)
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
    
    def test_sync_orders_places_both_orders_with_correct_offsets(self):
        """Test sync_orders places both PEG MID orders with correct offsets"""
        self._seed(30)
        self.trader.open_symbol_buys = 0
        self.trader.open_symbol_sells = 0
        self.trader.pending_buy = False
        self.trader.pending_sell = False
        
        captured_orders = []
        def capture_order(order_id, contract, order):
            captured_orders.append(order)
            return None
        
        self.trader.placeOrder = Mock(side_effect=capture_order)
        
        # Call the real sync_orders method
        self.trader.sync_orders()
        
        # Should place both BUY and SELL orders
        self.assertEqual(len(captured_orders), 2, f"Expected 2 orders, got {len(captured_orders)}")
        
        # Find buy and sell orders
        buy_order = next((order for order in captured_orders if order.action == "BUY"), None)
        sell_order = next((order for order in captured_orders if order.action == "SELL"), None)
        
        self.assertIsNotNone(buy_order)
        self.assertIsNotNone(sell_order)
        
        # Verify BUY order
        self.assertEqual(buy_order.totalQuantity, 70)
        self.assertEqual(buy_order.lmtPrice, 149.95)
        self.assertEqual(buy_order.orderType, "PEG MID")
        self.assertEqual(buy_order.midOffsetAtWhole, 0.01)
        
        # Verify SELL order
        self.assertEqual(sell_order.totalQuantity, 30)
        self.assertEqual(sell_order.lmtPrice, 150.05)
        self.assertEqual(sell_order.orderType, "PEG MID")
        self.assertEqual(sell_order.midOffsetAtWhole, 0.01)
    
    def test_sync_orders_with_half_penny_offset(self):
        """Test sync_orders uses half-penny offset when midpoint is on half-penny"""
        self.trader._bid = 150.00
        self.trader._ask = 150.01  # Half-penny midpoint
        self._seed(30)
        self.trader.open_symbol_buys = 0
        self.trader.open_symbol_sells = 0
        self.trader.pending_buy = False
        self.trader.pending_sell = False
        
        captured_orders = []
        def capture_order(order_id, contract, order):
            captured_orders.append(order)
            return None
        
        self.trader.placeOrder = Mock(side_effect=capture_order)
        
        self.trader.sync_orders()
        
        self.assertEqual(len(captured_orders), 2)
        
        buy_order = next((order for order in captured_orders if order.action == "BUY"), None)
        sell_order = next((order for order in captured_orders if order.action == "SELL"), None)
        
        # Both orders should use half-penny offset
        self.assertEqual(buy_order.midOffsetAtHalf, 0.015)
        self.assertEqual(sell_order.midOffsetAtHalf, 0.015)
    
    def test_sync_orders_places_only_buy_when_no_position(self):
        """Test sync_orders places only BUY order when position is 0"""
        self._seed(0)
        self.trader.open_symbol_buys = 0
        self.trader.open_symbol_sells = 0
        self.trader.pending_buy = False
        self.trader.pending_sell = False
        self.trader.sell_order_id = None
        
        captured_orders = []
        def capture_order(order_id, contract, order):
            captured_orders.append(order)
            return None
        
        self.trader.placeOrder = Mock(side_effect=capture_order)
        
        self.trader.sync_orders()
        
        self.assertEqual(len(captured_orders), 1)
        buy_order = captured_orders[0]
        self.assertEqual(buy_order.action, "BUY")
        self.assertEqual(buy_order.totalQuantity, 100)
    
    def test_sync_orders_places_only_sell_when_at_max(self):
        """Test sync_orders places only SELL order when at max position"""
        self._seed(100)
        self.trader.open_symbol_buys = 0
        self.trader.open_symbol_sells = 0
        self.trader.pending_buy = False
        self.trader.pending_sell = False
        self.trader.buy_order_id = None
        
        captured_orders = []
        def capture_order(order_id, contract, order):
            captured_orders.append(order)
            return None
        
        self.trader.placeOrder = Mock(side_effect=capture_order)
        
        self.trader.sync_orders()
        
        self.assertEqual(len(captured_orders), 1)
        sell_order = captured_orders[0]
        self.assertEqual(sell_order.action, "SELL")
        self.assertEqual(sell_order.totalQuantity, 100)
    
    def test_sync_orders_cancels_buy_when_at_max(self):
        """Test sync_orders cancels BUY order when position at max"""
        self._seed(100)
        self.trader.buy_order_id = 500
        self.trader.sell_order_id = None
        self.trader.open_symbol_buys = 1
        self.trader.pending_buy = False
        
        self.trader.sync_orders()
        
        # Should cancel BUY order (buy_qty = 0)
        self.trader.cancelOrder.assert_called_once()
        # Should still place SELL order
        self.assertEqual(self.trader.placeOrder.call_count, 1)
    
    def test_sync_orders_cancels_sell_when_no_position(self):
        """Test sync_orders cancels SELL order when position is 0"""
        self._seed(0)
        self.trader.sell_order_id = 501
        self.trader.buy_order_id = None
        self.trader.open_symbol_sells = 1
        self.trader.pending_sell = False
        
        self.trader.sync_orders()
        
        # Should cancel SELL order (sell_qty = 0)
        self.trader.cancelOrder.assert_called_once()
        # Should still place BUY order
        self.assertEqual(self.trader.placeOrder.call_count, 1)
    
    def test_sync_orders_respects_existing_buy_order(self):
        """Test sync_orders does not place new BUY when one already exists"""
        self._seed(30)
        self.trader.open_symbol_buys = 1  # Already has a buy order
        self.trader.buy_order_id = 500
        self.trader.open_symbol_sells = 0
        self.trader.pending_sell = False
        
        self.trader.sync_orders()
        
        # Should place SELL order only
        self.assertEqual(self.trader.placeOrder.call_count, 1)
        call_args = self.trader.placeOrder.call_args[0]
        self.assertEqual(call_args[2].action, "SELL")
    
    def test_sync_orders_respects_pending_buy(self):
        """Test sync_orders does not place new BUY when pending_buy is True"""
        self._seed(30)
        self.trader.pending_buy = True
        self.trader.open_symbol_buys = 0
        self.trader.open_symbol_sells = 0
        self.trader.pending_sell = False
        
        self.trader.sync_orders()
        
        # Should place SELL order only
        self.assertEqual(self.trader.placeOrder.call_count, 1)
        call_args = self.trader.placeOrder.call_args[0]
        self.assertEqual(call_args[2].action, "SELL")
    
    def test_sync_orders_does_nothing_when_not_ready(self):
        """Test sync_orders does nothing when not ready"""
        self.trader.ready_for_trading = False
        self._seed(30)
        
        self.trader.sync_orders()
        
        self.trader.placeOrder.assert_not_called()
        self.trader.cancelOrder.assert_not_called()
    
    def test_sync_orders_does_nothing_when_no_ref_price(self):
        """Test sync_orders does nothing when no ref_price"""
        self.trader.ref_price = None
        self._seed(30)
        
        self.trader.sync_orders()
        
        self.trader.placeOrder.assert_not_called()
        self.trader.cancelOrder.assert_not_called()
    
    def test_trigger_resync_debounce(self):
        """Test trigger_resync debounces rapid calls"""
        self.trader.isConnected = Mock(return_value=True)
        self.trader.request_positions_snapshot = Mock()
        self.trader.request_open_orders_snapshot = Mock()
        self.trader.maybe_sync_orders = Mock()
        
        # First call should trigger
        self.trader.trigger_resync()
        self.assertTrue(self.trader.sync_requested)
        self.trader.request_positions_snapshot.assert_called_once()
        self.trader.request_open_orders_snapshot.assert_called_once()
        
        # Reset mocks
        self.trader.request_positions_snapshot.reset_mock()
        self.trader.request_open_orders_snapshot.reset_mock()
        
        # Second call within debounce period should be ignored
        self.trader.trigger_resync()
        self.trader.request_positions_snapshot.assert_not_called()
        self.trader.request_open_orders_snapshot.assert_not_called()
    
    def test_trigger_resync_after_debounce_period(self):
        """Test trigger_resync triggers again after debounce period"""
        self.trader.isConnected = Mock(return_value=True)
        self.trader.request_positions_snapshot = Mock()
        self.trader.request_open_orders_snapshot = Mock()
        self.trader.maybe_sync_orders = Mock()
        
        # First call
        self.trader.trigger_resync()
        
        # Reset mocks
        self.trader.request_positions_snapshot.reset_mock()
        self.trader.request_open_orders_snapshot.reset_mock()
        self.trader.sync_requested = False
        
        # Mock time to advance beyond debounce period
        with patch('time.monotonic', return_value=1.0):
            self.trader.last_resync_request_ts = 0.0
            self.trader.trigger_resync()
            self.trader.request_positions_snapshot.assert_called_once()
            self.trader.request_open_orders_snapshot.assert_called_once()
    
    def test_trigger_resync_ignores_when_disconnected(self):
        """Test trigger_resync does nothing when disconnected"""
        self.trader.isConnected = Mock(return_value=False)
        self.trader.request_positions_snapshot = Mock()
        
        self.trader.trigger_resync()
        
        self.trader.request_positions_snapshot.assert_not_called()
    
    def test_maybe_sync_orders_when_ready(self):
        """Test maybe_sync_orders triggers sync when conditions met"""
        self.trader.sync_requested = True
        self.trader.position_snapshot_complete = True
        self.trader.open_orders_snapshot_complete = True
        self.trader.sync_orders = Mock()
        
        self.trader.maybe_sync_orders()
        
        self.assertFalse(self.trader.sync_requested)
        self.trader.sync_orders.assert_called_once()
    
    def test_maybe_sync_orders_when_not_ready(self):
        """Test maybe_sync_orders doesn't trigger when conditions not met"""
        self.trader.sync_requested = True
        self.trader.position_snapshot_complete = False
        self.trader.open_orders_snapshot_complete = True
        self.trader.sync_orders = Mock()
        
        self.trader.maybe_sync_orders()
        
        self.assertTrue(self.trader.sync_requested)
        self.trader.sync_orders.assert_not_called()
    
    def test_request_positions_snapshot(self):
        """Test requesting positions snapshot"""
        # Create a fresh trader to avoid state issues
        trader = Trader(self.config)
        trader.reqPositions = Mock()
        
        trader.position_snapshot_complete = True
        trader.position_size = 100
        
        trader.request_positions_snapshot()

        self.assertFalse(trader.position_snapshot_complete)
        self.assertEqual(trader.position_size, 100)
        trader.reqPositions.assert_called_once()
    
    def test_request_open_orders_snapshot(self):
        """Test requesting open orders snapshot"""
        # Create a fresh trader to avoid state issues
        trader = Trader(self.config)
        trader.reqOpenOrders = Mock()
        
        trader.open_orders_snapshot_complete = True
        trader.open_symbol_buys = 5
        trader.open_symbol_sells = 3
        
        trader.request_open_orders_snapshot()
        
        self.assertFalse(trader.open_orders_snapshot_complete)
        self.assertEqual(trader.open_symbol_buys, 0)
        self.assertEqual(trader.open_symbol_sells, 0)
        trader.reqOpenOrders.assert_called_once()
    
    def test_safe_cancel_order_single_arg(self):
        """Test safe_cancel_order with one-arg cancelOrder binding"""
        from ibkr_app_support import safe_cancel_order

        self.trader.isConnected = Mock(return_value=True)
        self.trader.serverVersion = Mock(return_value=157)
        self.trader.cancelOrder = Mock(side_effect=[TypeError(), None])
        safe_cancel_order(self.trader, 123)
        self.assertGreaterEqual(self.trader.cancelOrder.call_count, 1)

    def test_safe_cancel_order_legacy_empty_string_fallback(self):
        """Test safe_cancel_order falls back to cancelOrder(id, '')"""
        from ibkr_app_support import safe_cancel_order

        cancel_mock = Mock(side_effect=[TypeError(), None])
        self.trader.isConnected = Mock(return_value=True)
        self.trader.serverVersion = Mock(return_value=157)
        self.trader.cancelOrder = cancel_mock

        saved = sys.modules.pop("ibapi.order_cancel", None)
        try:
            safe_cancel_order(self.trader, 456)
        finally:
            if saved is not None:
                sys.modules["ibapi.order_cancel"] = saved

        cancel_mock.assert_has_calls([call(456), call(456, "")])
    
    def test_error_filtering_ignores_codes(self):
        """Test that error codes in ignored list are suppressed"""
        with patch('peg_mid.tprint') as mock_print:
            self.trader.error(0, "", 2104, "Market data farm connection is OK", "")
            mock_print.assert_not_called()
    
    def test_error_filtering_ignores_substrings(self):
        """Test that error messages with ignored substrings are suppressed"""
        with patch('peg_mid.tprint') as mock_print:
            self.trader.error(0, "", 10000, "Error: HMDS connection issue", "")
            mock_print.assert_not_called()
    
    def test_error_prints_non_ignored(self):
        """Test that non-ignored errors are printed"""
        with patch('peg_mid.tprint') as mock_print:
            self.trader.error(0, "", 500, "Critical error", "")
            mock_print.assert_called_once()
            call_args = mock_print.call_args[0][0]
            self.assertIn("Critical error", call_args)
    
    def test_run_loop_disconnected(self):
        """Test run loop when disconnected"""
        self.trader.isConnected = Mock(return_value=False)
        
        with patch('time.sleep') as mock_sleep:
            self.trader.run_loop()
            mock_sleep.assert_not_called()
    
    @patch('time.sleep')
    def test_run_loop_market_hours(self, mock_sleep):
        """Test run loop during market hours"""
        call_count = 0
        
        def is_connected_mock():
            nonlocal call_count
            call_count += 1
            return call_count < 3
        
        self.trader.isConnected = Mock(side_effect=is_connected_mock)
        self.trader.us_regular_hours = Mock(return_value=True)
        
        self.trader.request_positions_snapshot = Mock()
        self.trader.request_open_orders_snapshot = Mock()
        self.trader.maybe_sync_orders = Mock()
        
        self.trader.run_loop()
        
        self.trader.request_positions_snapshot.assert_called()
        self.trader.request_open_orders_snapshot.assert_called()
        self.trader.maybe_sync_orders.assert_called()
    
    @patch('time.sleep')
    def test_run_loop_outside_market_hours(self, mock_sleep):
        """Test run loop outside market hours - should not sync"""
        self.trader.isConnected = Mock(side_effect=[True, False])
        self.trader.us_regular_hours = Mock(return_value=False)
        
        self.trader.request_positions_snapshot = Mock()
        self.trader.request_open_orders_snapshot = Mock()
        self.trader.maybe_sync_orders = Mock()
        
        self.trader.run_loop()
        
        self.trader.request_positions_snapshot.assert_not_called()
        self.trader.request_open_orders_snapshot.assert_not_called()
        self.trader.maybe_sync_orders.assert_not_called()


class TestArgParser(unittest.TestCase):
    """Test command line argument parser"""
    
    def test_build_arg_parser(self):
        """Test argument parser creation"""
        parser = build_arg_parser()
        
        args = parser.parse_args([])
        self.assertTrue(hasattr(args, 'config'))
        self.assertTrue(hasattr(args, 'host'))
        self.assertTrue(hasattr(args, 'port'))
        self.assertTrue(hasattr(args, 'symbol'))
        self.assertTrue(hasattr(args, 'sec_type'))
        self.assertTrue(hasattr(args, 'max_pos'))
        self.assertTrue(hasattr(args, 'loop_seconds'))
        self.assertTrue(hasattr(args, 'buy_delta'))
        self.assertTrue(hasattr(args, 'sell_delta'))
        self.assertTrue(hasattr(args, 'peg_mid_offset_whole'))
        self.assertTrue(hasattr(args, 'peg_mid_offset_half'))
    
    def test_parse_args_with_peg_mid_offsets(self):
        """Test parsing command line arguments with PEG MID offsets"""
        parser = build_arg_parser()
        args = parser.parse_args([
            '--host', 'localhost',
            '--port', '7497',
            '--symbol', 'AAPL',
            '--max_pos', '200',
            '--buy_delta', '0.10',
            '--sell_delta', '0.15',
            '--peg_mid_offset_whole', '0.02',
            '--peg_mid_offset_half', '0.025'
        ])
        
        self.assertEqual(args.host, 'localhost')
        self.assertEqual(args.port, 7497)
        self.assertEqual(args.symbol, 'AAPL')
        self.assertEqual(args.max_pos, 200)
        self.assertEqual(args.buy_delta, 0.10)
        self.assertEqual(args.sell_delta, 0.15)
        self.assertEqual(args.peg_mid_offset_whole, 0.02)
        self.assertEqual(args.peg_mid_offset_half, 0.025)
    
    def test_default_config_path(self):
        """Test default config path"""
        parser = build_arg_parser()
        args = parser.parse_args([])
        self.assertTrue(args.config.endswith('peg_mid.json'))
    
    def test_peg_mid_offsets_are_optional_in_parser(self):
        """Test that peg_mid offsets are optional in command line"""
        parser = build_arg_parser()
        args = parser.parse_args(['--symbol', 'AAPL'])
        self.assertEqual(args.symbol, 'AAPL')


if __name__ == '__main__':
    unittest.main()