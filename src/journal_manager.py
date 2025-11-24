import json
from fsatomic import atomicWrite

from logger import getLogger
from utils import (
	isFileExists
)


class JournalManager:
	"""
	JournalManager class for managing persistent state of upgrade agent operations.

	The JournalManager maintains:
	- Command execution states for idempotent command processing
	- Current token for message sequencing
	- State transitions with timestamps	
	"""

	def __init__(self, journalPath):
		self.logger = getLogger('JournalManager')
		self.journalPath = journalPath

	def loadJournal(self):
		"""
		Load the journal from disk.

		If the journal file doesn't exist, initializes a new journal structure.
		Exits the application if loading fails due to corruption or I/O errors.
		"""
		try:
			if isFileExists(self.journalPath):
				with open(self.journalPath, 'r') as f:
					journal = json.load(f)
				self.logger.debug(f'Journal loaded from {self.journalPath}')
			else:
				self.logger.debug(f'Journal file not found, creating new journal')
				journal = {
					'version': 1,
					'currentToken': -1,
					'commands': {}
				}
			return journal
		except Exception as e:
			self.logger.error(f'Failed to load journal: {e}')
			raise

	def saveJournal(self, journal):
		"""
		Persist the journal atomically to disk.

		Uses atomic write to ensure readers never see a partially-written file.
		The data is serialized to JSON and written with fsync.

		Raises:
			Exception: If the save operation fails
		"""
		try:
			payload = json.dumps(journal, ensure_ascii=False).encode('utf-8')
			atomicWrite(self.journalPath, payload)
			self.logger.debug(f'Journal saved to {self.journalPath}')
		except Exception as e:
			self.logger.error(f'Failed to save journal: {e}')
			raise
