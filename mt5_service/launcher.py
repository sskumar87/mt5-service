"""
MT5 Platform Launcher
Launches multiple MT5 account services from configuration
"""
import asyncio
import logging
import yaml
import signal
import sys
from pathlib import Path
from typing import List
import time

sys.path.insert(0, str(Path(__file__).parent.parent))

from services.mt5_account_service import MT5AccountService
from core.broker import BrokerFactory

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PlatformLauncher:
    """Launches and manages all MT5 account services"""
    
    def __init__(self, config_path: str = "config.yaml"):
        self.config_path = config_path
        self.config = None
        self.services: List[MT5AccountService] = []
        self._running = False
    
    def load_config(self):
        """Load configuration from YAML file"""
        config_file = Path(self.config_path)
        
        if not config_file.exists():
            raise FileNotFoundError(
                f"Configuration file not found: {self.config_path}\n"
                "Please copy config.yaml.example to config.yaml and configure it."
            )
        
        with open(config_file, 'r') as f:
            self.config = yaml.safe_load(f)
        
        logger.info(f"Loaded configuration from {self.config_path}")
    
    def create_services(self):
        """Create MT5 account services from configuration"""
        broker_config = self.config.get('broker', {})
        accounts_config = self.config.get('accounts', [])
        
        if not accounts_config:
            raise ValueError("No accounts configured in config.yaml")
        
        # Create services for each enabled account
        for account in accounts_config:
            if not account.get('enabled', True):
                logger.info(f"Skipping disabled account: {account['account_id']}")
                continue
            
            # Create broker instance for this account with unique consumer group
            # CRITICAL: Each account needs its own consumer group to receive all messages
            broker = BrokerFactory.create_broker(
                broker_type=broker_config.get('type', 'redis'),
                host=broker_config.get('host', 'localhost'),
                port=broker_config.get('port', 6379),
                password=broker_config.get('password'),
                db=broker_config.get('db', 0),
                consumer_group=f"account_{account['account_id']}",  # Unique per account
                consumer_name=f"service_{account['account_id']}_{int(time.time())}",
                max_stream_length=broker_config.get('max_stream_length', 10000),
                claim_min_idle_time=broker_config.get('claim_min_idle_time', 60000)
            )
            
            # Create MT5 service
            service = MT5AccountService(
                account_id=account['account_id'],
                account_number=account['account_number'],
                password=account['password'],
                server=account['server'],
                broker=broker,
                path=account.get('mt5_path')
            )
            
            self.services.append(service)
            logger.info(f"Created service for account: {account['account_id']}")
        
        logger.info(f"Created {len(self.services)} MT5 account services")
    
    async def start(self):
        """Start all services"""
        logger.info("=" * 60)
        logger.info("Starting MT5 Trading Platform")
        logger.info("=" * 60)
        
        # Start all services
        start_tasks = [service.start() for service in self.services]
        await asyncio.gather(*start_tasks)
        
        self._running = True
        logger.info("All services started successfully")
        logger.info("=" * 60)
    
    async def stop(self):
        """Stop all services"""
        logger.info("=" * 60)
        logger.info("Stopping MT5 Trading Platform")
        logger.info("=" * 60)
        
        self._running = False
        
        # Stop all services
        stop_tasks = [service.stop() for service in self.services]
        await asyncio.gather(*stop_tasks, return_exceptions=True)
        
        logger.info("All services stopped")
        logger.info("=" * 60)
    
    async def run(self):
        """Run the platform"""
        try:
            # Load configuration
            self.load_config()
            
            # Create services
            self.create_services()
            
            # Start services
            await self.start()
            
            # Keep running
            logger.info("Platform is running. Press Ctrl+C to stop.")
            while self._running:
                await asyncio.sleep(1)
        
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        except Exception as e:
            logger.error(f"Error in platform: {e}", exc_info=True)
        finally:
            await self.stop()


def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info("Received signal, shutting down...")
    sys.exit(0)


async def main():
    """Main entry point"""
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create and run launcher
    launcher = PlatformLauncher(config_path="config.yaml")
    await launcher.run()


if __name__ == "__main__":
    asyncio.run(main())
