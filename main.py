#!/usr/bin/env python3

import asyncio
import argparse
import traceback
from upgradeagent import UpgradeAgent
from src.logger import getLogger

# Remote debugging support
try:
	import debugpy
	DEBUGPY_AVAILABLE = True
except ImportError:
	DEBUGPY_AVAILABLE = False


def setupRemoteDebugging(debug_host, debug_port, wait_for_client=False):
	"""
	Setup remote debugging with debugpy.
	
	Args:
		debug_host (str): The host to bind the debug server to
		debug_port (int): The port to bind the debug server to
		wait_for_client (bool): Whether to wait for a debugger to attach before continuing
		
	Returns:
		bool: True if debugging was successfully set up, False otherwise
	"""
	if not DEBUGPY_AVAILABLE:
		print("❌ debugpy not available. Install with: pip install debugpy")
		return False

	try:
		# Configure debugpy
		debugpy.configure(subProcess=False)

		# Listen for debugger connections
		debugpy.listen((debug_host, debug_port))
		print(f"🐛 Debug server started on {debug_host}:{debug_port}")
		print(f"   Connect your debugger to {debug_host}:{debug_port}")

		if wait_for_client:
			print("⏳ Waiting for debugger to attach...")
			debugpy.wait_for_client()
			print("✅ Debugger attached!")
		else:
			print("🔄 Continuing without waiting for debugger")

		return True

	except Exception as e:
		print(f"❌ Failed to setup remote debugging: {e}")
		return False


def parse_arguments():
	"""
	Parse command line arguments for the upgrade agent.
	
	Returns:
		argparse.Namespace: Parsed command line arguments
	"""
	parser = argparse.ArgumentParser(
		description='NVMesh Upgrade Agent with remote debugging support',
		formatter_class=argparse.RawDescriptionHelpFormatter
	)

	parser.add_argument(
		'--debug',
		action='store_true',
		help='Enable remote debugging with debugpy'
	)

	parser.add_argument(
		'--wait-for-debugger',
		action='store_true',
		help='Wait for debugger to attach before starting agent'
	)

	parser.add_argument(
		'--debug-host',
		type=str,
		default='0.0.0.0',
		help='Host address for debug server (default: 0.0.0.0)'
	)

	parser.add_argument(
		'--debug-port',
		type=int,
		default=5678,
		help='Port for debug server (default: 5678)'
	)

	return parser.parse_args()


def main():
	"""
	Main entry point for the NVMesh Upgrade Agent.
	
	This function:
	1. Parses command line arguments
	2. Sets up remote debugging if requested
	3. Initializes and starts the upgrade agent
	4. Handles any exceptions that occur during execution
	"""
	logger = getLogger('Main')
	args = parse_arguments()

	# Setup remote debugging if requested
	if args.debug:
		success = setupRemoteDebugging(
			debug_host=args.debug_host,
			debug_port=args.debug_port,
			wait_for_client=args.wait_for_debugger
		)
		if not success:
			logger.warning("Continuing without debugging...")

	# Start the upgrade agent
	try:
		logger.info("Starting NVMesh Upgrade Agent...")
		upgradeAgent = UpgradeAgent()
		asyncio.run(upgradeAgent.start())
	except Exception as e:
		logger.error(f"Upgrade agent crashed: {e}")
		logger.error(f"Traceback: {traceback.format_exc()}")
		traceback.print_exc()
		raise


if __name__ == "__main__":
	main()

