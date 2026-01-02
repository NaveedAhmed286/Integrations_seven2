"""
Wrapper for Google Sheets service.
Includes timeout, retry, response validation, error translation.
"""
import asyncio
from typing import List, Dict, Any, Optional
import json
import gspread
from gspread.exceptions import APIError, SpreadsheetNotFound
from oauth2client.service_account import ServiceAccountCredentials

from app.errors import ExternalServiceError
from app.utils.retry import async_retry
from app.config import config
from app.logger import logger


class GoogleSheetsService:
    """
    Wrapper for Google Sheets API.
    Business logic never calls Google Sheets directly.
    """
    
    def __init__(self):
        self.credentials_json = config.GOOGLE_SHEETS_CREDENTIALS_JSON
        self.client: Optional[gspread.Client] = None
        self.is_available = False  # Start as False, set to True after successful init
        self._initialized = False
    
    async def initialize(self):
        """Initialize Google Sheets client (called after startup)."""
        if not self.credentials_json:
            logger.warning("Google Sheets service not configured (no credentials)")
            self.is_available = False
            self._initialized = True
            return
        
        try:
            # Parse and validate credentials JSON
            creds_dict = json.loads(self.credentials_json)
            
            # Validate required Google service account fields
            required_fields = ["type", "project_id", "private_key_id", 
                              "private_key", "client_email", "client_id"]
            missing_fields = [f for f in required_fields if f not in creds_dict]
            
            if missing_fields:
                logger.error(f"Google Sheets credentials missing fields: {missing_fields}")
                self.is_available = False
                self._initialized = True
                return
            
            # Initialize using asyncio.to_thread (simpler than ThreadPoolExecutor)
            self.client = await asyncio.to_thread(
                self._initialize_sync,
                creds_dict
            )
            
            logger.info("Google Sheets service initialized successfully")
            self.is_available = True
            
        except json.JSONDecodeError as e:
            logger.error(f"Invalid Google Sheets credentials JSON: {e}")
            self.is_available = False
        except Exception as e:
            logger.error(f"Failed to initialize Google Sheets: {e}")
            self.is_available = False
        finally:
            self._initialized = True
    
    def _initialize_sync(self, creds_dict: Dict[str, Any]) -> gspread.Client:
        """Synchronous initialization for gspread."""
        scope = ["https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive"]
        
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(
            creds_dict, scope
        )
        
        return gspread.authorize(credentials)
    
    @async_retry(exceptions=(APIError, ConnectionError))
    async def append_to_sheet(self, spreadsheet_id: str, worksheet_name: str, 
                             data: List[Dict[str, Any]]) -> int:
        """
        Append data to Google Sheet.
        
        Args:
            spreadsheet_id: Google Spreadsheet ID
            worksheet_name: Worksheet name
            data: List of dictionaries to append
            
        Returns:
            Number of rows appended
            
        Raises:
            ExternalServiceError: If Google Sheets API fails
        """
        if not self.is_available:
            raise ExternalServiceError("Google Sheets service not available")
        
        if not data:
            return 0
        
        try:
            # Use asyncio.to_thread for async operation
            result = await asyncio.to_thread(
                self._append_to_sheet_sync,
                spreadsheet_id, worksheet_name, data
            )
            
            logger.info(f"Appended {len(data)} rows to {worksheet_name}")
            return result
            
        except SpreadsheetNotFound as e:
            raise ExternalServiceError(f"Spreadsheet not found: {str(e)}") from e
        except APIError as e:
            raise ExternalServiceError(f"Google Sheets API error: {str(e)}") from e
        except Exception as e:
            raise ExternalServiceError(f"Unexpected Google Sheets error: {str(e)}") from e
    
    def _append_to_sheet_sync(self, spreadsheet_id: str, worksheet_name: str,
                             data: List[Dict[str, Any]]) -> int:
        """Synchronous append operation."""
        spreadsheet = self.client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet(worksheet_name)
        
        # Get all values to check if sheet is empty
        existing_data = worksheet.get_all_values()
        
        # Determine headers
        if not existing_data:
            # Sheet is empty, use headers from first data row
            headers = list(data[0].keys())
            worksheet.append_row(headers)
        else:
            # Sheet has data, use existing headers
            headers = existing_data[0]
        
        # Append data rows
        rows_appended = 0
        for row in data:
            values = [str(row.get(key, "")) for key in headers]
            worksheet.append_row(values)
            rows_appended += 1
        
        return rows_appended
    
    async def append_rows(self, rows: List[Dict[str, Any]]) -> bool:
        """Simplified append for your existing code to use config.GOOGLE_SHEETS_SPREADSHEET_ID"""
        try:
            # Use the spreadsheet ID from config
            spreadsheet_id = config.GOOGLE_SHEETS_SPREADSHEET_ID
            if not spreadsheet_id:
                logger.warning("No Google Sheets spreadsheet ID configured")
                return False
            
            await self.append_to_sheet(
                spreadsheet_id=spreadsheet_id,
                worksheet_name="Sheet1",
                data=rows
            )
            return True
        except Exception as e:
            logger.error(f"Failed to append rows: {e}")
            return False
    
    @async_retry(exceptions=(APIError, ConnectionError))
    async def read_from_sheet(self, spreadsheet_id: str, worksheet_name: str,
                             range_name: Optional[str] = None) -> List[List[Any]]:
        """
        Read data from Google Sheet.
        
        Args:
            spreadsheet_id: Google Spreadsheet ID
            worksheet_name: Worksheet name
            range_name: Optional range (e.g., "A1:C10")
            
        Returns:
            List of rows
        """
        if not self.is_available:
            raise ExternalServiceError("Google Sheets service not available")
        
        try:
            result = await asyncio.to_thread(
                self._read_from_sheet_sync,
                spreadsheet_id, worksheet_name, range_name
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to read from Google Sheet: {e}")
            return []
    
    def _read_from_sheet_sync(self, spreadsheet_id: str, worksheet_name: str,
                             range_name: Optional[str] = None) -> List[List[Any]]:
        """Synchronous read operation."""
        spreadsheet = self.client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet(worksheet_name)
        
        if range_name:
            return worksheet.get(range_name)
        else:
            return worksheet.get_all_values()


# Global service instance
google_sheets_service = GoogleSheetsService()
