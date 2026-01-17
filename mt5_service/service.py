"""
MT5 Account Service
Connects to a single MT5 instance and processes trading events
"""
import asyncio
import logging
import time
from typing import Optional, Dict, Any, Set
from datetime import datetime
import MetaTrader5 as mt5

# Import from skav-trading-platform (core package)
from skav_trading import (
    MessageBroker, BrokerFactory,
    NewOrderMessage, ModifyOrderMessage, CancelOrderMessage,
    ClosePositionMessage, AccountStatusMessage, OrderExecutionMessage,
    OrderType, OrderStatus, MessageType, deserialize_message, ErrorMessage
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MT5AccountService:
    """Service for managing a single MT5 account"""
    
    def __init__(self, account_id: str, account_number: int, password: str, 
                 server: str, broker: MessageBroker, path: Optional[str] = None):
        self.account_id = account_id
        self.account_number = account_number
        self.password = password
        self.server = server
        self.mt5_path = path
        self.broker = broker
        
        self._connected = False
        self._running = False
        self._processed_messages: Set[str] = set()  # For idempotency
        self._max_processed_cache = 10000
        
        # Subscribed topics
        self.topics = [
            'orders/new_order',
            'orders/modify_order',
            'orders/cancel_order',
            'orders/close_position',
        ]
        
        logger.info(f"Initialized MT5 Account Service for {account_id}")
        logger.info(f"Using consumer group: account_{account_id} (ensures this account receives all messages)")
    
    async def start(self):
        """Start the service"""
        logger.info(f"Starting MT5 Account Service for {self.account_id}")
        
        # Connect to MT5
        await self._connect_mt5()
        
        # Connect to message broker
        await self.broker.connect()
        
        # Subscribe to topics
        await self.broker.subscribe(self.topics, self._handle_message)
        
        self._running = True
        
        # Start background tasks
        asyncio.create_task(self._periodic_status_update())
        asyncio.create_task(self._health_check_loop())
        
        logger.info(f"MT5 Account Service {self.account_id} started successfully")
    
    async def stop(self):
        """Stop the service"""
        logger.info(f"Stopping MT5 Account Service for {self.account_id}")
        self._running = False
        
        # Unsubscribe from topics
        await self.broker.unsubscribe(self.topics)
        
        # Disconnect from broker
        await self.broker.disconnect()
        
        # Shutdown MT5
        if self._connected:
            mt5.shutdown()
            self._connected = False
        
        logger.info(f"MT5 Account Service {self.account_id} stopped")
    
    async def _connect_mt5(self):
        """Connect to MT5 terminal"""
        try:
            # Initialize MT5
            if self.mt5_path:
                if not mt5.initialize(self.mt5_path):
                    raise Exception(f"MT5 initialize failed: {mt5.last_error()}")
            else:
                if not mt5.initialize():
                    raise Exception(f"MT5 initialize failed: {mt5.last_error()}")
            
            # Login to account
            if not mt5.login(self.account_number, password=self.password, server=self.server):
                raise Exception(f"MT5 login failed: {mt5.last_error()}")
            
            self._connected = True
            logger.info(f"Connected to MT5 account {self.account_number}")
            
        except Exception as e:
            logger.error(f"Failed to connect to MT5: {e}")
            raise
    
    async def _handle_message(self, topic: str, message_data: str):
        """Handle incoming messages from broker"""
        try:
            # Deserialize message
            message = deserialize_message(message_data)
            
            # Check idempotency
            if message.message_id in self._processed_messages:
                logger.debug(f"Skipping already processed message: {message.message_id}")
                return
            
            # Check if message is targeted to this account
            if hasattr(message, 'target_accounts') and message.target_accounts:
                if self.account_id not in message.target_accounts:
                    logger.debug(f"Message not targeted to this account: {message.message_id}")
                    return
            
            logger.info(f"Processing message {message.message_id} from {topic}")
            
            # Route to appropriate handler
            if isinstance(message, NewOrderMessage):
                await self._handle_new_order(message)
            elif isinstance(message, ModifyOrderMessage):
                await self._handle_modify_order(message)
            elif isinstance(message, CancelOrderMessage):
                await self._handle_cancel_order(message)
            elif isinstance(message, ClosePositionMessage):
                await self._handle_close_position(message)
            else:
                logger.warning(f"Unknown message type: {type(message)}")
            
            # Mark as processed
            self._processed_messages.add(message.message_id)
            
            # Cleanup old processed messages
            if len(self._processed_messages) > self._max_processed_cache:
                # Remove oldest 20%
                to_remove = list(self._processed_messages)[:2000]
                self._processed_messages -= set(to_remove)
        
        except Exception as e:
            logger.error(f"Error handling message: {e}", exc_info=True)
            await self._publish_error(str(e), message_data)
    
    async def _handle_new_order(self, message: NewOrderMessage):
        """Execute new order"""
        start_time = time.time()
        
        try:
            # Map order type
            order_type_map = {
                OrderType.BUY: mt5.ORDER_TYPE_BUY,
                OrderType.SELL: mt5.ORDER_TYPE_SELL,
                OrderType.BUY_LIMIT: mt5.ORDER_TYPE_BUY_LIMIT,
                OrderType.SELL_LIMIT: mt5.ORDER_TYPE_SELL_LIMIT,
                OrderType.BUY_STOP: mt5.ORDER_TYPE_BUY_STOP,
                OrderType.SELL_STOP: mt5.ORDER_TYPE_SELL_STOP,
            }
            
            # Get symbol info
            symbol_info = mt5.symbol_info(message.symbol)
            if symbol_info is None:
                raise Exception(f"Symbol {message.symbol} not found")
            
            if not symbol_info.visible:
                if not mt5.symbol_select(message.symbol, True):
                    raise Exception(f"Failed to select symbol {message.symbol}")
            
            # Prepare order request
            request = {
                "action": mt5.TRADE_ACTION_DEAL,
                "symbol": message.symbol,
                "volume": message.volume,
                "type": order_type_map[message.order_type],
                "deviation": 20,
                "magic": message.magic_number or 234000,
                "comment": message.comment or f"Order {message.message_id[:8]}",
                "type_time": mt5.ORDER_TIME_GTC,
                "type_filling": mt5.ORDER_FILLING_IOC,
            }
            
            # Add price for limit/stop orders
            if message.price is not None:
                request["price"] = message.price
            else:
                # Use current market price
                if message.order_type in [OrderType.BUY, OrderType.BUY_LIMIT, OrderType.BUY_STOP]:
                    request["price"] = mt5.symbol_info_tick(message.symbol).ask
                else:
                    request["price"] = mt5.symbol_info_tick(message.symbol).bid
            
            # Add SL/TP
            if message.stop_loss is not None:
                request["sl"] = message.stop_loss
            if message.take_profit is not None:
                request["tp"] = message.take_profit
            
            # Send order
            result = mt5.order_send(request)
            execution_time = (time.time() - start_time) * 1000
            
            # Publish execution result
            if result.retcode == mt5.TRADE_RETCODE_DONE:
                await self._publish_execution_result(
                    message.message_id,
                    OrderStatus.EXECUTED,
                    ticket=result.order,
                    symbol=message.symbol,
                    volume=message.volume,
                    price=result.price,
                    execution_time_ms=execution_time
                )
                logger.info(f"Order executed successfully: ticket={result.order}, price={result.price}")
            else:
                await self._publish_execution_result(
                    message.message_id,
                    OrderStatus.FAILED,
                    symbol=message.symbol,
                    volume=message.volume,
                    error_code=result.retcode,
                    error_message=result.comment,
                    execution_time_ms=execution_time
                )
                logger.error(f"Order failed: {result.comment} (code: {result.retcode})")
        
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            await self._publish_execution_result(
                message.message_id,
                OrderStatus.FAILED,
                error_message=str(e),
                execution_time_ms=execution_time
            )
            logger.error(f"Error executing order: {e}", exc_info=True)
    
    async def _handle_modify_order(self, message: ModifyOrderMessage):
        """Modify existing order"""
        try:
            request = {
                "action": mt5.TRADE_ACTION_SLTP,
                "position": message.ticket,
            }
            
            if message.stop_loss is not None:
                request["sl"] = message.stop_loss
            if message.take_profit is not None:
                request["tp"] = message.take_profit
            
            result = mt5.order_send(request)
            
            if result.retcode == mt5.TRADE_RETCODE_DONE:
                logger.info(f"Order {message.ticket} modified successfully")
            else:
                logger.error(f"Failed to modify order: {result.comment}")
        
        except Exception as e:
            logger.error(f"Error modifying order: {e}", exc_info=True)
    
    async def _handle_cancel_order(self, message: CancelOrderMessage):
        """Cancel pending order"""
        try:
            request = {
                "action": mt5.TRADE_ACTION_REMOVE,
                "order": message.ticket,
            }
            
            result = mt5.order_send(request)
            
            if result.retcode == mt5.TRADE_RETCODE_DONE:
                logger.info(f"Order {message.ticket} cancelled successfully")
            else:
                logger.error(f"Failed to cancel order: {result.comment}")
        
        except Exception as e:
            logger.error(f"Error cancelling order: {e}", exc_info=True)
    
    async def _handle_close_position(self, message: ClosePositionMessage):
        """Close position"""
        try:
            # Get position info
            positions = mt5.positions_get(ticket=message.ticket)
            if not positions:
                raise Exception(f"Position {message.ticket} not found")
            
            position = positions[0]
            
            # Determine close action
            if position.type == mt5.POSITION_TYPE_BUY:
                order_type = mt5.ORDER_TYPE_SELL
                price = mt5.symbol_info_tick(position.symbol).bid
            else:
                order_type = mt5.ORDER_TYPE_BUY
                price = mt5.symbol_info_tick(position.symbol).ask
            
            # Prepare close request
            request = {
                "action": mt5.TRADE_ACTION_DEAL,
                "symbol": position.symbol,
                "volume": message.volume or position.volume,
                "type": order_type,
                "position": message.ticket,
                "price": price,
                "deviation": 20,
                "magic": position.magic,
                "comment": f"Close {message.message_id[:8]}",
                "type_time": mt5.ORDER_TIME_GTC,
                "type_filling": mt5.ORDER_FILLING_IOC,
            }
            
            result = mt5.order_send(request)
            
            if result.retcode == mt5.TRADE_RETCODE_DONE:
                logger.info(f"Position {message.ticket} closed successfully")
            else:
                logger.error(f"Failed to close position: {result.comment}")
        
        except Exception as e:
            logger.error(f"Error closing position: {e}", exc_info=True)
    
    async def _publish_execution_result(self, order_id: str, status: OrderStatus, **kwargs):
        """Publish order execution result"""
        result = OrderExecutionMessage(
            message_id="",
            message_type=MessageType.NEW_ORDER,
            timestamp="",
            source_service=f"mt5_service_{self.account_id}",
            account_id=self.account_id,
            order_id=order_id,
            status=status,
            **kwargs
        )
        
        await self.broker.publish('account/executions', result.to_json())
    
    async def _publish_error(self, error: str, context: str):
        """Publish error message"""
        error_msg = ErrorMessage(
            message_id="",
            message_type=MessageType.ERROR,
            timestamp="",
            source_service=f"mt5_service_{self.account_id}",
            error_type="EXECUTION_ERROR",
            error_message=error,
            context={"original_message": context},
            severity="ERROR"
        )
        
        await self.broker.publish('system/errors', error_msg.to_json())
    
    async def _periodic_status_update(self):
        """Periodically publish account status"""
        while self._running:
            try:
                await asyncio.sleep(10)  # Every 10 seconds
                
                if self._connected:
                    account_info = mt5.account_info()
                    if account_info:
                        status = AccountStatusMessage(
                            message_id="",
                            message_type=MessageType.ACCOUNT_STATUS,
                            timestamp="",
                            source_service=f"mt5_service_{self.account_id}",
                            account_id=self.account_id,
                            balance=account_info.balance,
                            equity=account_info.equity,
                            margin=account_info.margin,
                            free_margin=account_info.margin_free,
                            margin_level=account_info.margin_level,
                            profit=account_info.profit,
                            open_positions=len(mt5.positions_get() or []),
                            pending_orders=len(mt5.orders_get() or []),
                            connected=True
                        )
                        
                        await self.broker.publish('account/status', status.to_json())
            
            except Exception as e:
                logger.error(f"Error in status update: {e}", exc_info=True)
    
    async def _health_check_loop(self):
        """Periodic health check"""
        while self._running:
            try:
                await asyncio.sleep(60)  # Every minute
                
                # Check MT5 connection
                if self._connected:
                    account_info = mt5.account_info()
                    if account_info is None:
                        logger.warning("MT5 connection lost, attempting reconnect...")
                        await self._connect_mt5()
                
                # Check broker connection
                broker_healthy = await self.broker.health_check()
                if not broker_healthy:
                    logger.warning("Broker connection unhealthy")
            
            except Exception as e:
                logger.error(f"Health check error: {e}", exc_info=True)


# Example usage
async def main():
    """Example main function"""
    # Create broker
    broker = BrokerFactory.create_broker('redis', host='localhost', port=6379)
    
    # Create MT5 service
    service = MT5AccountService(
        account_id="account_1",
        account_number=12345678,
        password="your_password",
        server="YourBroker-Server",
        broker=broker
    )
    
    try:
        await service.start()
        
        # Keep running
        while True:
            await asyncio.sleep(1)
    
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        await service.stop()


if __name__ == "__main__":
    asyncio.run(main())
