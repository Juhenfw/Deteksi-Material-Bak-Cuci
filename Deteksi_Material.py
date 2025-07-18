import cv2
import numpy as np
from datetime import datetime, timedelta
import threading
import time
from collections import defaultdict
from ultralytics import YOLO
import mysql.connector
from mysql.connector import Error
import json
import queue
import logging
import logging.handlers
import sys
import signal
import traceback
from urllib.parse import quote
import torch
import psutil
import gc
import schedule
from http.server import BaseHTTPRequestHandler, HTTPServer
import socketserver
from io import BytesIO

# Enhanced logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.handlers.RotatingFileHandler("material_dipping.log", maxBytes=50*1024*1024, backupCount=5, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("MaterialDippingV4")

# Global control variables
running = True
reconnect_delay = 5
max_queue_size = 100
frame_counter = 0

# Enhanced frame queue
frame_queue = queue.Queue(maxsize=max_queue_size)

class StreamingHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/':
            self.send_response(301)
            self.send_header('Location', '/stream1.mjpg')
            self.end_headers()
        elif self.path == '/stream1.mjpg':
            self.send_response(200)
            self.send_header('Age', 0)
            self.send_header('Cache-Control', 'no-cache, private')
            self.send_header('Pragma', 'no-cache')
            self.send_header('Content-Type', 'multipart/x-mixed-replace; boundary=FRAME')
            self.end_headers()
            try:
                while True:
                    if hasattr(self.server, 'current_frame') and self.server.current_frame is not None:
                        # Encode frame ke JPEG
                        ret, jpeg = cv2.imencode('.jpg', self.server.current_frame, 
                                               [cv2.IMWRITE_JPEG_QUALITY, 85])
                        if ret:
                            frame_bytes = jpeg.tobytes()
                            self.wfile.write(b'--FRAME\r\n')
                            self.send_header('Content-Type', 'image/jpeg')
                            self.send_header('Content-Length', len(frame_bytes))
                            self.end_headers()
                            self.wfile.write(frame_bytes)
                            self.wfile.write(b'\r\n')
                    time.sleep(0.033)  # ~30 FPS
            except Exception as e:
                logger.error(f"Streaming error: {e}")
        else:
            self.send_error(404)
            self.end_headers()

class StreamingServer(socketserver.ThreadingMixIn, HTTPServer):
    allow_reuse_address = True
    daemon_threads = True
    current_frame = None

    def update_frame(self, frame):
        self.current_frame = frame.copy()

class SystemWatchdog:
    """Enhanced system monitoring and health check"""
    def __init__(self):
        self.last_activity = time.time()
        self.freeze_threshold = 15
        self.memory_threshold = 90
        self.is_monitoring = True
        
    def update_activity(self):
        self.last_activity = time.time()
        
    def check_system_health(self):
        current_time = time.time()
        
        if current_time - self.last_activity > self.freeze_threshold:
            logger.critical("System freeze detected! Attempting recovery...")
            self.emergency_recovery()
            
        memory_percent = psutil.virtual_memory().percent
        if memory_percent > self.memory_threshold:
            logger.warning(f"High memory usage: {memory_percent}%")
            gc.collect()
            
    def emergency_recovery(self):
        """Emergency recovery procedure"""
        global frame_queue
        
        while not frame_queue.empty():
            try:
                frame_queue.get_nowait()
            except queue.Empty:
                break
        
        self.last_activity = time.time()
        logger.info("Emergency recovery completed")

watchdog = SystemWatchdog()

def setup_signal_handlers():
    """Setup graceful shutdown handlers"""
    def signal_handler(sig, frame):
        global running
        logger.info("Shutdown signal received")
        running = False
        watchdog.is_monitoring = False
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

def create_rtsp_url_robust(ip, port="554", username="admin", password="admin", path=""):
    """Create optimized RTSP URL"""
    encoded_password = quote(password)
    if path:
        url = f"rtsp://{username}:{encoded_password}@{ip}:{port}/{path}"
    else:
        url = f"rtsp://{username}:{encoded_password}@{ip}:{port}"
    return url

class DailyTimer:
    """Daily resetting timer for each area"""
    def __init__(self, area_name):
        self.area_name = area_name
        self.daily_total_seconds = 0
        self.last_reset_date = datetime.now().date()
        self.current_session_start = None
        self.is_active = False
        
    def start_session(self, start_time):
        """Start a new timing session"""
        if not self.is_active:
            self.current_session_start = start_time
            self.is_active = True
            self.check_daily_reset()
            
    def end_session(self, end_time):
        """End current timing session and add to daily total"""
        if self.is_active and self.current_session_start:
            session_duration = (end_time - self.current_session_start).total_seconds()
            self.daily_total_seconds += session_duration
            self.is_active = False
            self.current_session_start = None
            return session_duration
        return 0
    
    def get_current_session_duration(self):
        """Get current session duration if active"""
        if self.is_active and self.current_session_start:
            return (datetime.now() - self.current_session_start).total_seconds()
        return 0
    
    def get_daily_total(self):
        """Get total daily duration including current session"""
        total = self.daily_total_seconds
        if self.is_active:
            total += self.get_current_session_duration()
        return total
    
    def check_daily_reset(self):
        """Check if daily reset is needed"""
        current_date = datetime.now().date()
        if current_date > self.last_reset_date:
            logger.info(f"[{self.area_name}] Daily timer reset - Previous total: {self.format_duration(self.daily_total_seconds)}")
            self.daily_total_seconds = 0
            self.last_reset_date = current_date
    
    def force_daily_reset(self):
        """Force daily reset (called at midnight)"""
        logger.info(f"[{self.area_name}] Forced daily timer reset - Total: {self.format_duration(self.daily_total_seconds)}")
        self.daily_total_seconds = 0
        self.last_reset_date = datetime.now().date()
    
    def format_duration(self, seconds):
        """Format duration from seconds to HH:MM:SS"""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"

class IndependentAreaTracker:
    """Independent tracking system for each material area with daily timer"""
    def __init__(self, area_name, area_bbox, db_config, area_name_db):
        self.area_name = area_name
        self.area_bbox = area_bbox
        self.db_config = db_config
        self.area_name_db = area_name_db
        self.area_number = int(area_name.split()[1])  # Extract number from "Area X"
        
        # Independent area state
        self.is_material_present = False
        self.material_start_time = None
        self.last_detection_time = None
        self.material_entry_time = None
        
        # Daily timer
        self.daily_timer = DailyTimer(area_name)
        
        # Detection parameters
        self.material_lost_threshold = 1.0  # seconds
        
        logger.info(f"Independent tracker with daily timer initialized for {area_name}")
    
    def is_inside_area(self, obj_bbox):
        """Check if object center is inside this area"""
        obj_center_x = (obj_bbox[0] + obj_bbox[2]) / 2
        obj_center_y = (obj_bbox[1] + obj_bbox[3]) / 2
        
        return (self.area_bbox[0] <= obj_center_x <= self.area_bbox[2] and 
                self.area_bbox[1] <= obj_center_y <= self.area_bbox[3])
    
    def update_detection(self, current_time, material_detected):
        """Update area detection state and handle immediate logging"""
        if material_detected:
            self.last_detection_time = current_time
            
            # Material entry - start timing
            if not self.is_material_present:
                self.is_material_present = True
                self.material_start_time = current_time
                self.material_entry_time = current_time.strftime("%H:%M:%S")
                self.daily_timer.start_session(current_time)
                logger.info(f"[{self.area_name}] Material entered at {self.material_entry_time}")
        
        else:
            # Check if material should be considered lost
            if self.is_material_present and self.last_detection_time:
                time_since_last = (current_time - self.last_detection_time).total_seconds()
                
                if time_since_last >= self.material_lost_threshold:
                    # Material exit - immediate logging
                    self.handle_material_exit(current_time)
    
    def handle_material_exit(self, exit_time):
        """Handle material exit with immediate database logging"""
        if not self.is_material_present or not self.material_start_time:
            return
        
        # End timer session
        session_duration = self.daily_timer.end_session(exit_time)
        
        # Format duration to HH:MM:SS
        hours = int(session_duration // 3600)
        minutes = int((session_duration % 3600) // 60)
        seconds = int(session_duration % 60)
        time_format = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        
        # Immediate database logging
        self.save_area_data(
            time_format=time_format,
            time_in=self.material_start_time,
            created_at=exit_time,
            remark="in_use"
        )
        
        # Reset state
        self.is_material_present = False
        self.material_start_time = None
        self.material_entry_time = None
        self.last_detection_time = None
        
        daily_total = self.daily_timer.get_daily_total()
        logger.info(f"[{self.area_name}] Material exited - Session: {time_format} - Daily Total: {self.daily_timer.format_duration(daily_total)}")
    
    def save_area_data(self, time_format, time_in, created_at, remark):
        """Save area data immediately to database"""
        try:
            conn = mysql.connector.connect(**self.db_config)
            
            if conn.is_connected():
                cursor = conn.cursor()
                
                shift_info = self.get_current_shift(time_in)
                time_in_str = time_in.strftime('%Y-%m-%d %H:%M:%S')
                created_at_str = created_at.strftime('%Y-%m-%d %H:%M:%S')
                log_date = self.get_log_date_for_shift(time_in)
                
                # Hitung session_duration dalam detik
                session_duration = (created_at - time_in).total_seconds()

                # Adjust Sesuai nama Kolom data yang dipakai
                insert_query = """
                INSERT INTO iot_lacquerings (area, time, time_int, time_in, shift, remark, no_area, log_date, created_at) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                values = (
                    self.area_name_db,
                    time_format,
                    int(session_duration),
                    time_in_str,
                    shift_info,
                    remark,
                    int(self.area_number),
                    log_date,
                    created_at_str
                )
                
                cursor.execute(insert_query, values)
                conn.commit()
                
                logger.info(f"[{self.area_name}] Data saved: {remark} - {time_format}")
                
                cursor.close()
                conn.close()
                
        except Exception as e:
            logger.error(f"[{self.area_name}] Error saving data: {e}")
    
    def get_current_shift(self, dt=None):
        """Get current shift information"""
        if dt is None:
            dt = datetime.now()
        hour = dt.hour
        minute = dt.minute
        # Shift 1: 07:10 - 16:00
        if (hour == 7 and minute >= 10) or (7 < hour < 16):
            return "Shift_1"
        # Shift 2: 16:00 - 00:15 (melewati tengah malam)
        elif (16 <= hour < 24) or (hour == 0 and minute <= 15):
            return "Shift_2"
        # Shift 3: 00:16 - 06:59
        else:
            return "Shift_3"
    
    def get_log_date_for_shift(self, dt=None):
        """Get log date based on shift with special handling for Shift 2 midnight"""
        if dt is None:
            dt = datetime.now()
        
        shift = self.get_current_shift(dt)
        hour = dt.hour
        minute = dt.minute
        
        # Shift 3: 00:16 - 06:59 (gunakan tanggal sebelumnya)
        if shift == "Shift_3":
            return (dt.date() - timedelta(days=1))
        # Shift 2: khusus jam 00:00 - 00:15 (gunakan tanggal sebelumnya)
        elif shift == "Shift_2" and hour == 0 and minute <= 15:
            return (dt.date() - timedelta(days=1))
        # Semua kondisi lainnya (gunakan tanggal hari ini)
        else:
            return dt.date()
    
    def get_status(self):
        """Get current area status"""
        return "Material" if self.is_material_present else "Kosong"
    
    def get_current_duration(self):
        """Get current material duration if active"""
        return self.daily_timer.get_current_session_duration()
    
    def get_daily_total_duration(self):
        """Get total daily duration"""
        return self.daily_timer.get_daily_total()
    
    def reset_daily_timer(self):
        """Reset daily timer"""
        self.daily_timer.force_daily_reset()

class FreetimeTracker:
    """Global freetime tracking system with daily timer"""
    def __init__(self, db_config, area_name_db):
        self.db_config = db_config
        self.area_name_db = area_name_db
        
        # Freetime state
        self.is_freetime_active = False
        self.freetime_start_time = None
        self.freetime_threshold = 2.0  # seconds before considering freetime
        self.all_empty_start = None
        
        # Daily timer for freetime
        self.daily_timer = DailyTimer("Freetime")
        
        logger.info("Freetime tracker with daily timer initialized")
    
    def update_freetime_status(self, all_areas_empty, current_time):
        """Update global freetime status"""
        if all_areas_empty:
            if self.all_empty_start is None:
                self.all_empty_start = current_time
            else:
                # Check if freetime threshold reached
                empty_duration = (current_time - self.all_empty_start).total_seconds()
                
                if empty_duration >= self.freetime_threshold and not self.is_freetime_active:
                    # Start freetime
                    self.is_freetime_active = True
                    self.freetime_start_time = current_time
                    self.daily_timer.start_session(current_time)
                    logger.info(f"Freetime started at {current_time.strftime('%H:%M:%S')}")
        else:
            # Material detected - end freetime if active
            if self.is_freetime_active:
                self.end_freetime(current_time, "material_detected")
            
            self.all_empty_start = None
    
    def end_freetime(self, end_time, reason="unknown"):
        """End freetime and save data"""
        if not self.is_freetime_active or not self.freetime_start_time:
            return
        
        # End timer session
        session_duration = self.daily_timer.end_session(end_time)
        
        # Format duration
        hours = int(session_duration // 3600)
        minutes = int((session_duration % 3600) // 60)
        seconds = int(session_duration % 60)
        time_format = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        
        # Save freetime data
        self.save_freetime_data(
            time_format=time_format,
            time_in=self.freetime_start_time,
            created_at=end_time,
            remark="freetime"
        )
        
        # Reset state
        self.is_freetime_active = False
        self.freetime_start_time = None
        
        daily_total = self.daily_timer.get_daily_total()
        logger.info(f"Freetime ended - Session: {time_format} - Daily Total: {self.daily_timer.format_duration(daily_total)} - Reason: {reason}")
    
    def save_freetime_data(self, time_format, time_in, created_at, remark):
        """Save freetime data to database"""
        try:
            conn = mysql.connector.connect(**self.db_config)
            
            if conn.is_connected():
                cursor = conn.cursor()
                
                shift_info = self.get_current_shift(time_in)
                time_in_str = time_in.strftime('%Y-%m-%d %H:%M:%S')
                created_at_str = created_at.strftime('%Y-%m-%d %H:%M:%S')
                log_date = self.get_log_date_for_shift(time_in)
                
                # Hitung session_duration dalam detik
                session_duration = (created_at - time_in).total_seconds()

                # Adjust Sesuai nama Kolom data yang dipakai
                insert_query = """
                INSERT INTO iot_lacquerings (area, time, time_int, time_in, shift, remark, no_area, log_date, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                values = (
                    self.area_name_db,
                    time_format,
                    int(session_duration),  # Tambahkan time_int
                    time_in_str,
                    shift_info,
                    remark,
                    int(0),  # no_area for freetime
                    log_date,
                    created_at_str
                )
                
                cursor.execute(insert_query, values)
                conn.commit()
                
                logger.info(f"Freetime data saved: {time_format}")
                
                cursor.close()
                conn.close()
                
        except Exception as e:
            logger.error(f"Error saving freetime data: {e}")
    
    def get_current_shift(self, dt=None):
        """Get current shift information"""
        if dt is None:
            dt = datetime.now()
        hour = dt.hour
        minute = dt.minute
        # Shift 1: 07:10 - 16:00
        if (hour == 7 and minute >= 10) or (7 < hour < 16):
            return "Shift_1"
        # Shift 2: 16:00 - 00:15 (melewati tengah malam)
        elif (16 <= hour < 24) or (hour == 0 and minute <= 15):
            return "Shift_2"
        # Shift 3: 00:16 - 06:59
        else:
            return "Shift_3"
    
    def get_log_date_for_shift(self, dt=None):
        """Get log date based on shift with special handling for Shift 2 midnight"""
        if dt is None:
            dt = datetime.now()
        
        shift = self.get_current_shift(dt)
        hour = dt.hour
        minute = dt.minute
        
        # Shift 3: 00:16 - 06:59 (gunakan tanggal sebelumnya)
        if shift == "Shift_3":
            return (dt.date() - timedelta(days=1))
        # Shift 2: khusus jam 00:00 - 00:15 (gunakan tanggal sebelumnya)
        elif shift == "Shift_2" and hour == 0 and minute <= 15:
            return (dt.date() - timedelta(days=1))
        # Semua kondisi lainnya (gunakan tanggal hari ini)
        else:
            return dt.date()
    
    def get_current_duration(self):
        """Get current freetime duration"""
        return self.daily_timer.get_current_session_duration()
    
    def get_daily_total_duration(self):
        """Get total daily freetime duration"""
        return self.daily_timer.get_daily_total()
    
    def reset_daily_timer(self):
        """Reset daily timer"""
        self.daily_timer.force_daily_reset()

class GloveTracker:
    """Sarung tangan detection and tracking system with daily timer"""
    def __init__(self, area_polygon, db_config, area_name_db):
        self.area_polygon = area_polygon
        self.db_config = db_config
        self.area_name_db = area_name_db
        
        # Glove state
        self.is_glove_detected = False
        self.glove_start_time = None
        self.last_glove_detection = None
        self.glove_lost_threshold = 1.0  # seconds
        
        # Integration with freetime
        self.was_freetime_before_glove = False
        
        # Daily timer for glove usage
        self.daily_timer = DailyTimer("Glove")
        
        logger.info("Glove tracker with daily timer initialized")
    
    def is_inside_area(self, obj_bbox):
        """Check if glove is inside polygon detection area"""
        # Ambil center point dari object bbox
        obj_center_x = (obj_bbox[0] + obj_bbox[2]) / 2
        obj_center_y = (obj_bbox[1] + obj_bbox[3]) / 2
        
        # Gunakan cv2.pointPolygonTest untuk mengecek apakah point ada di dalam polygon
        point = (int(obj_center_x), int(obj_center_y))
        result = cv2.pointPolygonTest(self.area_polygon, point, False)
        
        # result >= 0 berarti point ada di dalam atau di tepi polygon
        return result >= 0

    
    def update_glove_detection(self, current_time, glove_detected, freetime_tracker):
        """Update glove detection with freetime integration"""
        if glove_detected:
            self.last_glove_detection = current_time
            
            # Glove entry
            if not self.is_glove_detected:
                # Check if freetime was active before glove detection
                if freetime_tracker.is_freetime_active:
                    self.was_freetime_before_glove = True
                    # End freetime and start in_use tracking
                    freetime_tracker.end_freetime(current_time, "glove_detected")
                    self.daily_timer.start_session(current_time)
                else:
                    self.was_freetime_before_glove = False
                
                # Start glove tracking
                self.is_glove_detected = True
                self.glove_start_time = current_time
                logger.info(f"Glove detected at {current_time.strftime('%H:%M:%S')} - Was freetime: {self.was_freetime_before_glove}")
        
        else:
            # Check if glove should be considered lost
            if self.is_glove_detected and self.last_glove_detection:
                time_since_last = (current_time - self.last_glove_detection).total_seconds()
                
                if time_since_last >= self.glove_lost_threshold:
                    self.handle_glove_exit(current_time)
    
    def handle_glove_exit(self, exit_time):
        """Handle glove exit with conditional logging"""
        if not self.is_glove_detected or not self.glove_start_time:
            return
        
        session_duration = 0
        
        # Only log as in_use if it was preceded by freetime
        if self.was_freetime_before_glove:
            # End timer session
            session_duration = self.daily_timer.end_session(exit_time)
            
            # Format duration
            hours = int(session_duration // 3600)
            minutes = int((session_duration % 3600) // 60)
            seconds = int(session_duration % 60)
            time_format = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
            
            # Save as in_use
            self.save_glove_data(
                time_format=time_format,
                time_in=self.glove_start_time,
                created_at=exit_time,
                remark="in_use"
            )
            
            daily_total = self.daily_timer.get_daily_total()
            logger.info(f"Glove exited - Logged as in_use: {time_format} - Daily Total: {self.daily_timer.format_duration(daily_total)}")
        else:
            logger.info(f"Glove exited - Not logged (no preceding freetime)")
        
        # Reset state
        self.is_glove_detected = False
        self.glove_start_time = None
        self.last_glove_detection = None
        self.was_freetime_before_glove = False
    
    def save_glove_data(self, time_format, time_in, created_at, remark):
        """Save glove data to database"""
        try:
            conn = mysql.connector.connect(**self.db_config)
            
            if conn.is_connected():
                cursor = conn.cursor()
                
                shift_info = self.get_current_shift(time_in)
                time_in_str = time_in.strftime('%Y-%m-%d %H:%M:%S')
                created_at_str = created_at.strftime('%Y-%m-%d %H:%M:%S')
                log_date = self.get_log_date_for_shift(time_in)
                
                # Hitung session_duration dalam detik
                session_duration = (created_at - time_in).total_seconds()

                # Adjust Sesuai nama Kolom data yang dipakai
                insert_query = """
                INSERT INTO iot_lacquerings (area, time, time_int, time_in, shift, remark, no_area, log_date, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                values = (
                    self.area_name_db,
                    time_format,
                    int(session_duration),  # Tambahkan time_int
                    time_in_str,
                    shift_info,
                    remark,
                    int(5),  # no_area for glove area
                    log_date,
                    created_at_str
                )
                
                cursor.execute(insert_query, values)
                conn.commit()
                
                logger.info(f"Glove data saved: {remark} - {time_format}")
                
                cursor.close()
                conn.close()
                
        except Exception as e:
            logger.error(f"Error saving glove data: {e}")
    
    def get_current_shift(self, dt=None):
        """Get current shift information"""
        if dt is None:
            dt = datetime.now()
        hour = dt.hour
        minute = dt.minute
        # Shift 1: 07:10 - 16:00
        if (hour == 7 and minute >= 10) or (7 < hour < 16):
            return "Shift_1"
        # Shift 2: 16:00 - 00:15 (melewati tengah malam)
        elif (16 <= hour < 24) or (hour == 0 and minute <= 15):
            return "Shift_2"
        # Shift 3: 00:16 - 06:59
        else:
            return "Shift_3"
    
    def get_log_date_for_shift(self, dt=None):
        """Get log date based on shift with special handling for Shift 2 midnight"""
        if dt is None:
            dt = datetime.now()
        
        shift = self.get_current_shift(dt)
        hour = dt.hour
        minute = dt.minute
        
        # Shift 3: 00:16 - 06:59 (gunakan tanggal sebelumnya)
        if shift == "Shift_3":
            return (dt.date() - timedelta(days=1))
        # Shift 2: khusus jam 00:00 - 00:15 (gunakan tanggal sebelumnya)
        elif shift == "Shift_2" and hour == 0 and minute <= 15:
            return (dt.date() - timedelta(days=1))
        # Semua kondisi lainnya (gunakan tanggal hari ini)
        else:
            return dt.date()
    
    def get_status(self):
        """Get current glove status"""
        return "Sarung_Tangan" if self.is_glove_detected else "Kosong"
    
    def get_daily_total_duration(self):
        """Get total daily glove usage duration"""
        return self.daily_timer.get_daily_total()
    
    def reset_daily_timer(self):
        """Reset daily timer"""
        self.daily_timer.force_daily_reset()

class EnhancedMaterialDippingDetection:
    """Main detection system with timer-based tracking and large status display"""
    def __init__(self, model_path, video_source, db_config=None, is_rtsp=False, rtsp_config=None, area_name_db="bak_alkali"):
        try:
            # Initialize YOLO model
            self.model = YOLO(model_path)
            self.video_source = video_source
            self.db_config = db_config
            self.is_rtsp = is_rtsp
            self.rtsp_config = rtsp_config
            self.area_name_db = area_name_db
            
            # GPU setup
            if torch.cuda.is_available():
                torch.cuda.set_device(0)
                self.model = self.model.to('cuda')
                logger.info(f"Using CUDA device: {torch.cuda.get_device_name(0)}")
            else:
                self.model = self.model.to('cpu')
                logger.info("Using CPU for inference")
            
            # Define detection areas
            self.material_areas = {
                "Area 1": {"bbox": (552, 292, 674, 422), "color": (0, 255, 0)},
                "Area 2": {"bbox": (709, 393, 835, 526), "color": (0, 255, 0)},
                "Area 3": {"bbox": (870, 529, 1052, 700), "color": (0, 255, 0)},
                "Area 4": {"bbox": (1101, 690, 1291, 874), "color": (0, 255, 0)}
            }
            
            self.glove_area = {
                "polygon": np.array([
                    [396, 482],   # titik kiri atas
                    [761, 180],  # titik kanan atas
                    [1609, 614],  # titik kanan bawah
                    [1283, 1080],    # titik kiri bawah
                    [970, 1080]
                ], dtype=np.int32),
                "color": (0, 165, 255)
            }

            
            # Initialize independent trackers with timers
            self.area_trackers = {}
            for area_name, area_info in self.material_areas.items():
                self.area_trackers[area_name] = IndependentAreaTracker(
                    area_name=area_name,
                    area_bbox=area_info["bbox"],
                    db_config=self.db_config,
                    area_name_db=self.area_name_db
                )
            
            # Initialize freetime tracker with timer
            self.freetime_tracker = FreetimeTracker(
                db_config=self.db_config,
                area_name_db=self.area_name_db
            )
            
            # Initialize glove tracker with timer
            self.glove_tracker = GloveTracker(
                area_polygon=self.glove_area["polygon"],
                db_config=self.db_config,
                area_name_db=self.area_name_db
            )

            
            # Setup scheduled tasks
            self.setup_scheduled_tasks()
            
            # Performance monitoring
            self.fps = 0
            self.last_frame_time = time.time()
            self.fps_counter = 0
            
            # Database connection
            self.db_connection = None
            if self.db_config:
                self.connect_database()
            
            logger.info("Enhanced Material Dipping Detection with Large Status Display initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize detection system: {e}")
            logger.error(traceback.format_exc())
            raise
    
    def setup_scheduled_tasks(self):
        """Setup scheduled tasks for daily reset"""
        # Schedule daily reset at midnight
        schedule.every().day.at("00:00").do(self.reset_all_daily_timers)
        
        # Start scheduler thread
        scheduler_thread = threading.Thread(target=self.run_scheduler, daemon=True)
        scheduler_thread.start()
        
        logger.info("Scheduled tasks initialized - Daily reset at midnight")
    
    def run_scheduler(self):
        """Run scheduled tasks"""
        while running:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    
    def reset_all_daily_timers(self):
        """Reset all daily timers at midnight"""
        logger.info("=== DAILY TIMER RESET ===")
        
        for area_name, tracker in self.area_trackers.items():
            tracker.reset_daily_timer()
        
        self.freetime_tracker.reset_daily_timer()
        self.glove_tracker.reset_daily_timer()
        
        logger.info("All daily timers reset successfully")
    
    def connect_database(self):
        """Connect to database with retry logic"""
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                self.db_connection = mysql.connector.connect(
                    host=self.db_config.get('host', 'localhost'),
                    database=self.db_config.get('database', 'dipping_system'),
                    user=self.db_config.get('user', 'root'),
                    password=self.db_config.get('password', ''),
                    autocommit=True,
                    connection_timeout=10,
                    buffered=True
                )
                
                if self.db_connection.is_connected():
                    logger.info(f"Database connected successfully (attempt {attempt + 1})")
                    return
                    
            except Error as e:
                logger.error(f"Database connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    logger.critical("Failed to connect to database after all attempts")
    
    def get_bak_status(self):
        """Determine if Bak is In Use or Not In Use"""
        # Check if any material area has material
        any_material = any(tracker.is_material_present for tracker in self.area_trackers.values())
        
        # Check if gloves are detected
        gloves_detected = self.glove_tracker.is_glove_detected
        
        # Bak is in use if any material is detected OR gloves are detected
        return any_material or gloves_detected
    
    def process_frame(self, frame):
        """Enhanced frame processing with large status display"""
        try:
            start_time = time.time()
            current_time = datetime.now()
            
            # Update FPS
            self.fps_counter += 1
            if start_time - self.last_frame_time >= 1.0:
                self.fps = self.fps_counter / (start_time - self.last_frame_time)
                self.fps_counter = 0
                self.last_frame_time = start_time
            
            # Frame validation
            if frame is None or frame.size == 0:
                logger.warning("Invalid frame received")
                return np.zeros((1080, 1920, 3), dtype=np.uint8)
            
            # Ensure consistent frame size
            if frame.shape[0] != 1080 or frame.shape[1] != 1920:
                frame = cv2.resize(frame, (1920, 1080))
            
            # Run YOLO detection
            with torch.no_grad():
                results = self.model(frame, verbose=False)
            
            # Process detections
            self.update_all_trackers(results, current_time)
            
            # Apply visualization
            annotated_frame = results[0].plot()
            final_frame = self.draw_enhanced_visualization(annotated_frame)
            
            # Update watchdog
            watchdog.update_activity()
            
            return final_frame
            
        except Exception as e:
            logger.error(f"Error in process_frame: {e}")
            logger.error(traceback.format_exc())
            return frame
    
    def update_all_trackers(self, results, current_time):
        """Update all tracking systems"""
        # Initialize detection flags
        material_detections = {area_name: False for area_name in self.area_trackers.keys()}
        glove_detected = False
        
        # Process YOLO detections
        if results[0].boxes is not None:
            for box in results[0].boxes:
                bbox = box.xyxy[0].cpu().numpy()
                cls = int(box.cls[0])
                conf = float(box.conf[0])
                
                # Material detection (class 0)
                if cls == 0 and conf > 0.6:
                    for area_name, tracker in self.area_trackers.items():
                        if tracker.is_inside_area(bbox):
                            material_detections[area_name] = True
                            break
                
                # Glove detection (class 2)
                elif cls == 2 and conf > 0.6:
                    if self.glove_tracker.is_inside_area(bbox):
                        glove_detected = True
        
        # Update independent area trackers
        for area_name, tracker in self.area_trackers.items():
            tracker.update_detection(current_time, material_detections[area_name])
        
        # Update freetime tracker
        all_areas_empty = not any(material_detections.values())
        self.freetime_tracker.update_freetime_status(all_areas_empty, current_time)
        
        # Update glove tracker
        self.glove_tracker.update_glove_detection(current_time, glove_detected, self.freetime_tracker)
    
    def draw_enhanced_visualization(self, frame):
        """Draw enhanced visualization with status display as highest priority"""
        frame_height, frame_width = frame.shape[:2]
        
        # Gambar semua elemen background terlebih dahulu
        self.draw_material_areas(frame)
        self.draw_glove_area(frame)
        self.draw_side_panels(frame, frame_width, frame_height)
        self.draw_fps_counter(frame)
        
        # Gambar header panel (tapi hindari area status)
        self.draw_header_panel(frame, frame_width)
        
        # Gambar status display TERAKHIR (prioritas tertinggi)
        self.draw_large_status_display(frame, frame_width, frame_height)
        
        return frame

    
    def draw_large_status_display(self, frame, frame_width, frame_height):
        """Draw large prominent status display with solid background"""
        # Status logic tetap sama
        bak_in_use = self.get_bak_status()
        status_text = "BAK DIGUNAKAN" if bak_in_use else "BAK KOSONG"
        status_color = (0, 255, 0) if bak_in_use else (0, 0, 255)
    
        # Calculate text size and position
        font_scale = 3.0
        thickness = 8
        font = cv2.FONT_HERSHEY_SIMPLEX
        
        # Get text size
        text_size = cv2.getTextSize(status_text, font, font_scale, thickness)[0]
        
        # Center position
        text_x = (frame_width - text_size[0]) // 2
        text_y = 200
        
        # Draw background rectangle dengan solid background
        padding = 30
        bg_x1 = text_x - padding
        bg_y1 = text_y - text_size[1] - padding
        bg_x2 = text_x + text_size[0] + padding
        bg_y2 = text_y + padding
        
        # Background hitam solid (bukan semi-transparan)
        cv2.rectangle(frame, (bg_x1, bg_y1), (bg_x2, bg_y2), (0, 0, 0), -1)
        
        # Draw border
        cv2.rectangle(frame, (bg_x1, bg_y1), (bg_x2, bg_y2), status_color, 5)
        
        # Enhanced blinking effect untuk "BAK IN USE"
        if bak_in_use:
            # Gunakan milliseconds untuk kelap-kelip yang lebih halus
            current_ms = int(time.time() * 1000)
            blink_interval = 500  # 500ms = 0.5 detik
            is_blink_on = (current_ms // blink_interval) % 2 == 0
            
            if is_blink_on:
                # State normal - hijau
                text_color = status_color
                glow_color = (255, 255, 255)
            else:
                # State kelap-kelip - putih terang
                text_color = (255, 255, 255)
                glow_color = (0, 255, 0)
            
            # Draw glow effect
            cv2.putText(frame, status_text, (text_x, text_y), font, font_scale, glow_color, thickness + 4)
            # Draw main text
            cv2.putText(frame, status_text, (text_x, text_y), font, font_scale, text_color, thickness)
            
            # Optional: tambahkan efek border yang ikut berkedip
            border_color = glow_color if is_blink_on else status_color
            cv2.rectangle(frame, (bg_x1, bg_y1), (bg_x2, bg_y2), border_color, 5)
            
        else:
            # Untuk "BAK NOT IN USE" - tidak berkedip
            cv2.putText(frame, status_text, (text_x, text_y), font, font_scale, (255, 255, 255), thickness + 2)
            cv2.putText(frame, status_text, (text_x, text_y), font, font_scale, status_color, thickness)
        
        # Status details tetap sama
        detail_y = text_y + 60
        detail_font_scale = 1.2
        detail_thickness = 3
        
        active_areas = [name for name, tracker in self.area_trackers.items() if tracker.is_material_present]
        glove_active = self.glove_tracker.is_glove_detected
        
        if bak_in_use:
            details = []
            if active_areas:
                details.append(f"Material: {', '.join([area.replace('Area ', 'A') for area in active_areas])}")
            if glove_active:
                details.append("Gloves: Detected")
            
            detail_text = " | ".join(details)
            detail_size = cv2.getTextSize(detail_text, font, detail_font_scale, detail_thickness)[0]
            detail_x = (frame_width - detail_size[0]) // 2
            
            # Background untuk detail text
            detail_bg_x1 = detail_x - 20
            detail_bg_y1 = detail_y - 25
            detail_bg_x2 = detail_x + detail_size[0] + 20
            detail_bg_y2 = detail_y + 10
            
            cv2.rectangle(frame, (detail_bg_x1, detail_bg_y1), (detail_bg_x2, detail_bg_y2), (0, 0, 0), -1)
            cv2.putText(frame, detail_text, (detail_x, detail_y), font, detail_font_scale, (255, 255, 255), detail_thickness)
        else:
            detail_text = "All Areas Empty"
            detail_size = cv2.getTextSize(detail_text, font, detail_font_scale, detail_thickness)[0]
            detail_x = (frame_width - detail_size[0]) // 2
            
            cv2.rectangle(frame, (detail_x - 20, detail_y - 25), (detail_x + detail_size[0] + 20, detail_y + 10), (0, 0, 0), -1)
            cv2.putText(frame, detail_text, (detail_x, detail_y), font, detail_font_scale, (200, 200, 200), detail_thickness)
    def draw_header_panel(self, frame, frame_width):
        """Draw main header panel with timer info"""
        panel_height = 120
        
        # Background
        overlay = frame.copy()
        cv2.rectangle(overlay, (0, 0), (frame_width, panel_height), (20, 20, 20), -1)
        alpha = 0.85
        cv2.addWeighted(overlay, alpha, frame, 1 - alpha, 0, frame)
        
        # Current time
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cv2.putText(frame, f"TIME: {current_time}", (20, 35), 
                   cv2.FONT_HERSHEY_SIMPLEX, 0.8, (255, 255, 255), 2)
        
        # Source status
        status_text = "RTSP LIVE" if self.is_rtsp else "VIDEO FILE"
        status_color = (0, 255, 0) if self.is_rtsp else (255, 255, 0)
        cv2.putText(frame, f"SOURCE: {status_text}", (20, 65), 
                   cv2.FONT_HERSHEY_SIMPLEX, 0.6, status_color, 2)
        
        # Database status
        db_status = "CONNECTED" if self.db_connection and self.db_connection.is_connected() else "DISCONNECTED"
        db_color = (0, 255, 0) if self.db_connection and self.db_connection.is_connected() else (0, 0, 255)
        cv2.putText(frame, f"DATABASE: {db_status}", (20, 90), 
                   cv2.FONT_HERSHEY_SIMPLEX, 0.6, db_color, 2)
        
        # Total daily time
        total_daily = sum(tracker.get_daily_total_duration() for tracker in self.area_trackers.values())
        total_formatted = self.format_duration(total_daily)
        cv2.putText(frame, f"TOTAL DAILY: {total_formatted}", (400, 35), 
                   cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 255, 255), 2)
        
        # Freetime status
        if self.freetime_tracker.is_freetime_active:
            freetime_duration = self.freetime_tracker.get_current_duration()
            freetime_text = f"FREETIME: {self.format_duration(freetime_duration)}"
            cv2.putText(frame, freetime_text, (400, 65), 
                       cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 165, 255), 2)
        else:
            freetime_daily = self.freetime_tracker.get_daily_total_duration()
            cv2.putText(frame, f"FREETIME DAILY: {self.format_duration(freetime_daily)}", (400, 65), 
                       cv2.FONT_HERSHEY_SIMPLEX, 0.6, (100, 165, 255), 2)
    
    def draw_material_areas(self, frame):
        """Draw material areas with timer information"""
        for area_name, area_info in self.material_areas.items():
            bbox = area_info["bbox"]
            tracker = self.area_trackers[area_name]
            
            # Area color based on status
            color = (0, 255, 0) if tracker.is_material_present else (100, 100, 100)
            
            # Draw area rectangle
            cv2.rectangle(frame, (bbox[0], bbox[1]), (bbox[2], bbox[3]), color, 3)
            
            # Area label with daily total
            label_height = 35
            cv2.rectangle(frame, (bbox[0], bbox[1] - label_height),
                         (bbox[2], bbox[1]), color, -1)
            
            status = tracker.get_status()
            daily_total = tracker.get_daily_total_duration()
            daily_formatted = tracker.daily_timer.format_duration(daily_total)
            label_text = f"{area_name}: {status} ({daily_formatted})"
            cv2.putText(frame, label_text, (bbox[0] + 8, bbox[1] - 10), 
                       cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255, 255, 255), 2)
            
            # Info panel below area
            self.draw_area_info_panel(frame, bbox, tracker)
    
    def draw_area_info_panel(self, frame, bbox, tracker):
        """Draw information panel below each area with timer info"""
        panel_y = bbox[3] + 5
        panel_height = 80
        
        # Background
        cv2.rectangle(frame, (bbox[0], panel_y), 
                     (bbox[2], panel_y + panel_height), (30, 30, 30), -1)
        cv2.rectangle(frame, (bbox[0], panel_y), 
                     (bbox[2], panel_y + panel_height), (80, 80, 80), 1)
        
        # Current session timer
        y_offset = panel_y + 18
        if tracker.is_material_present:
            duration = tracker.get_current_duration()
            timer_text = f"Session: {self.format_duration(duration)}"
            cv2.putText(frame, timer_text, (bbox[0] + 5, y_offset), 
                       cv2.FONT_HERSHEY_SIMPLEX, 0.45, (0, 255, 255), 2)
            
            if tracker.material_entry_time:
                entry_text = f"Entry: {tracker.material_entry_time}"
                cv2.putText(frame, entry_text, (bbox[0] + 5, y_offset + 18), 
                           cv2.FONT_HERSHEY_SIMPLEX, 0.4, (255, 255, 0), 1)
        else:
            cv2.putText(frame, "Session: --:--", (bbox[0] + 5, y_offset), 
                       cv2.FONT_HERSHEY_SIMPLEX, 0.45, (150, 150, 150), 2)
        
        # Daily total
        daily_total = tracker.get_daily_total_duration()
        daily_text = f"Daily: {tracker.daily_timer.format_duration(daily_total)}"
        cv2.putText(frame, daily_text, (bbox[0] + 5, y_offset + 36), 
                   cv2.FONT_HERSHEY_SIMPLEX, 0.45, (0, 255, 0), 1)
        
        # Reset time info
        reset_date = tracker.daily_timer.last_reset_date.strftime("%m/%d")
        cv2.putText(frame, f"Reset: {reset_date}", (bbox[0] + 5, y_offset + 54), 
                   cv2.FONT_HERSHEY_SIMPLEX, 0.35, (150, 150, 150), 1)
    
    def draw_glove_area(self, frame):
        """Draw glove detection area with timer info"""
        polygon = self.glove_area["polygon"]
        color = (0, 165, 255) if self.glove_tracker.is_glove_detected else (0, 140, 255)
        
        # Draw polygon area
        cv2.polylines(frame, [polygon], True, color, 2)
        
        # Optional: Fill polygon dengan transparansi
        if self.glove_tracker.is_glove_detected:
            overlay = frame.copy()
            cv2.fillPoly(overlay, [polygon], color)
            cv2.addWeighted(overlay, 0.2, frame, 0.8, 0, frame)
        
        # Label dengan daily total
        status = self.glove_tracker.get_status()
        daily_total = self.glove_tracker.get_daily_total_duration()
        daily_formatted = self.glove_tracker.daily_timer.format_duration(daily_total)
        label_text = f"Area 5 (Glove): {status} ({daily_formatted})"
        
        # Ambil titik paling atas untuk label
        top_point = polygon[np.argmin(polygon[:, 1])]
        cv2.putText(frame, label_text, (top_point[0] + 8, top_point[1] - 10), 
                cv2.FONT_HERSHEY_SIMPLEX, 0.6, color, 2)

    
    def draw_side_panels(self, frame, frame_width, frame_height):
        """Draw side information panels with timer information"""
        panel_width = 300
        panel_x = frame_width - panel_width - 15
        
        # Daily timer panel
        panel_y = 300  # Moved down to accommodate large status display
        panel_height = 180
        self.draw_panel_with_title(frame, panel_x, panel_y, panel_width, panel_height, 
                                  "DAILY TIMERS", (255, 255, 0))
        
        y_offset = panel_y + 45
        for area_name, tracker in self.area_trackers.items():
            area_short = area_name.replace("Area ", "A")
            daily_total = tracker.get_daily_total_duration()
            daily_formatted = tracker.daily_timer.format_duration(daily_total)
            timer_color = (0, 255, 255) if daily_total > 0 else (150, 150, 150)
            cv2.putText(frame, f"{area_short}: {daily_formatted}", (panel_x + 10, y_offset), 
                       cv2.FONT_HERSHEY_SIMPLEX, 0.55, timer_color, 2)
            y_offset += 25
        
        # Freetime daily
        freetime_daily = self.freetime_tracker.get_daily_total_duration()
        freetime_formatted = self.freetime_tracker.daily_timer.format_duration(freetime_daily)
        cv2.putText(frame, f"Free: {freetime_formatted}", (panel_x + 10, y_offset), 
                   cv2.FONT_HERSHEY_SIMPLEX, 0.55, (0, 165, 255), 2)
        y_offset += 25
        
        # Glove daily
        glove_daily = self.glove_tracker.get_daily_total_duration()
        glove_formatted = self.glove_tracker.daily_timer.format_duration(glove_daily)
        cv2.putText(frame, f"Glove: {glove_formatted}", (panel_x + 10, y_offset), 
                   cv2.FONT_HERSHEY_SIMPLEX, 0.55, (0, 165, 255), 2)
        
        # Status panel
        status_panel_y = panel_y + panel_height + 20
        status_panel_height = 120
        self.draw_panel_with_title(frame, panel_x, status_panel_y, panel_width, status_panel_height, 
                                  "SYSTEM STATUS", (255, 255, 255))
        
        y_offset = status_panel_y + 45
        
        # Active areas
        active_areas = sum(1 for tracker in self.area_trackers.values() if tracker.is_material_present)
        cv2.putText(frame, f"Active Areas: {active_areas}/4", (panel_x + 10, y_offset), 
                   cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 255, 255), 2)
        y_offset += 25
        
        # Current freetime status
        if self.freetime_tracker.is_freetime_active:
            freetime_duration = self.freetime_tracker.get_current_duration()
            cv2.putText(frame, f"Freetime: {self.format_duration(freetime_duration)}", (panel_x + 10, y_offset), 
                       cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 165, 255), 2)
        else:
            cv2.putText(frame, "Freetime: Inactive", (panel_x + 10, y_offset), 
                       cv2.FONT_HERSHEY_SIMPLEX, 0.6, (150, 150, 150), 2)
        y_offset += 25
        
        # Reset info
        reset_time = "00:00 Daily"
        cv2.putText(frame, f"Timer Reset: {reset_time}", (panel_x + 10, y_offset), 
                   cv2.FONT_HERSHEY_SIMPLEX, 0.5, (200, 200, 200), 1)
    
    def draw_panel_with_title(self, frame, x, y, width, height, title, title_color):
        """Helper function to draw panel with title"""
        # Background
        cv2.rectangle(frame, (x, y), (x + width, y + height), (40, 40, 40), -1)
        cv2.rectangle(frame, (x, y), (x + width, y + height), (100, 100, 100), 2)
        
        # Title
        title_height = 25
        cv2.rectangle(frame, (x + 2, y + 2), (x + width - 2, y + title_height), 
                     (title_color[0]//3, title_color[1]//3, title_color[2]//3), -1)
        cv2.putText(frame, title, (x + 8, y + 18), 
                   cv2.FONT_HERSHEY_SIMPLEX, 0.55, title_color, 2)
    
    def draw_fps_counter(self, frame):
        """Draw FPS counter"""
        fps_text = f"FPS: {self.fps:.1f}"
        text_size = cv2.getTextSize(fps_text, cv2.FONT_HERSHEY_SIMPLEX, 0.6, 2)[0]
        
        fps_x = frame.shape[1] - text_size[0] - 20
        fps_y = frame.shape[0] - 20
        
        cv2.rectangle(frame, (fps_x - 5, fps_y - text_size[1] - 5), 
                     (fps_x + text_size[0] + 5, fps_y + 5), (30, 30, 30), -1)
        
        fps_color = (0, 255, 0) if self.fps >= 20 else (0, 255, 255) if self.fps >= 10 else (0, 0, 255)
        cv2.putText(frame, fps_text, (fps_x, fps_y - 5), 
                   cv2.FONT_HERSHEY_SIMPLEX, 0.6, fps_color, 2)
    
    def format_duration(self, seconds):
        """Format duration from seconds to HH:MM:SS"""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    
    def cleanup(self):
        """Cleanup resources and save final data"""
        try:
            current_time = datetime.now()
            
            # Save any active area data
            for area_name, tracker in self.area_trackers.items():
                if tracker.is_material_present:
                    tracker.handle_material_exit(current_time)
                    logger.info(f"Final save for {area_name} during cleanup")
            
            # Save active freetime data
            if self.freetime_tracker.is_freetime_active:
                self.freetime_tracker.end_freetime(current_time, "system_shutdown")
                logger.info("Final freetime save during cleanup")
            
            # Save active glove data
            if self.glove_tracker.is_glove_detected:
                self.glove_tracker.handle_glove_exit(current_time)
                logger.info("Final glove save during cleanup")
            
            # Close database connection
            if self.db_connection and self.db_connection.is_connected():
                self.db_connection.close()
                logger.info("Database connection closed")
                
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

# Video capture function
def Receive(url, frame_queue):
    """Enhanced video capture with robust error handling"""
    global running, frame_counter
    frame_counter = 0
    skip_frames = 2
    reconnect_attempts = 0
    max_reconnect_attempts = 10
    backoff_factor = 1.5
    cap = None
    
    is_video_file = not url.startswith('rtsp://')
    target_fps = 30
    frame_delay = 0
    last_frame_time = time.time()
    
    logger.info(f"Starting video capture from {url}")
    logger.info(f"Source type: {'Video File' if is_video_file else 'RTSP Stream'}")
    
    while running:
        try:
            cap = cv2.VideoCapture(url)
            
            if not is_video_file:
                cap.set(cv2.CAP_PROP_BUFFERSIZE, 3)
                try:
                    cap.set(cv2.CAP_PROP_TIMEOUT, 15000)
                except AttributeError:
                    logger.warning("CAP_PROP_TIMEOUT not available")
            else:
                cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
            
            if not cap.isOpened():
                raise ValueError(f"Failed to open video source: {url}")
            
            if is_video_file:
                video_fps = cap.get(cv2.CAP_PROP_FPS)
                if video_fps > 0:
                    target_fps = video_fps
                    frame_delay = 1.0 / video_fps
                else:
                    frame_delay = 1.0 / 30
            
            logger.info("Video source connected successfully")
            reconnect_attempts = 0
            
            while running:
                current_time = time.time()
                
                if is_video_file and frame_delay > 0:
                    time_since_last_frame = current_time - last_frame_time
                    if time_since_last_frame < frame_delay:
                        sleep_time = frame_delay - time_since_last_frame
                        time.sleep(sleep_time)
                
                ret, frame = cap.read()
                if not ret:
                    if is_video_file:
                        logger.info("Video file ended, looping...")
                        cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                        continue
                    else:
                        logger.warning("Failed to receive frame")
                        break
                
                last_frame_time = time.time()
                frame_counter += 1
                
                watchdog.update_activity()
                
                if frame_counter % skip_frames == 0:
                    if frame.shape[0] != 1080 or frame.shape[1] != 1920:
                        frame = cv2.resize(frame, (1920, 1080))
                    
                    try:
                        frame_queue.put(frame, block=False)
                    except queue.Full:
                        try:
                            for _ in range(frame_queue.qsize() // 2):
                                frame_queue.get_nowait()
                            frame_queue.put(frame, block=False)
                        except queue.Empty:
                            frame_queue.put(frame, block=False)
            
            if cap:
                cap.release()
            logger.info("Video connection lost, attempting to reconnect")
            
        except Exception as e:
            logger.error(f"Video capture error: {e}")
            if cap:
                cap.release()
        
        if is_video_file:
            logger.info("Video file processing completed")
            break
        
        reconnect_attempts += 1
        if reconnect_attempts <= max_reconnect_attempts:
            delay = min(30, reconnect_delay * (backoff_factor ** (reconnect_attempts - 1)))
        else:
            delay = 30
        
        logger.info(f"Waiting {delay}s before reconnection attempt {reconnect_attempts}")
        
        if not running:
            break
        
        time.sleep(delay)
    
    logger.info("Video capture thread stopping")
    if cap:
        cap.release()

# Display function
def Display(detector, frame_queue, streaming_server=None, window_name="Enhanced Material Dipping Detection"):
    """Enhanced display with streaming capability"""
    global running
    logger.info("Starting display thread with streaming")
    
    # Setup OpenCV window (opsional, bisa dimatikan untuk headless)
    show_window = True  # Set False untuk headless mode
    if show_window:
        cv2.namedWindow(window_name, cv2.WINDOW_NORMAL)
        cv2.resizeWindow(window_name, 1920, 1080)
    
    is_video_file = not detector.video_source.startswith('rtsp://')
    wait_key_delay = 30 if is_video_file else 1
    
    frame_timeout = 5
    last_frame_time = time.time()
    
    while running:
        try:
            try:
                frame = frame_queue.get(timeout=frame_timeout)
                last_frame_time = time.time()
                
                # Process frame
                processed_frame = detector.process_frame(frame)
                
                # Update streaming server
                if streaming_server:
                    streaming_server.update_frame(processed_frame)
                
                # Show window (opsional)
                if show_window:
                    cv2.imshow(window_name, processed_frame)
                
                watchdog.update_activity()
                
            except queue.Empty:
                current_time = time.time()
                if current_time - last_frame_time > 10:
                    logger.warning("No frames received for 10 seconds")
                    warning_frame = np.zeros((1080, 1920, 3), dtype=np.uint8)
                    cv2.putText(warning_frame, "NO SIGNAL - RECONNECTING...", 
                              (400, 540), cv2.FONT_HERSHEY_SIMPLEX, 2, (0, 0, 255), 3)
                    
                    if streaming_server:
                        streaming_server.update_frame(warning_frame)
                    if show_window:
                        cv2.imshow(window_name, warning_frame)
                    last_frame_time = current_time
            
            if show_window:
                key = cv2.waitKey(wait_key_delay) & 0xFF
                
                if key == ord('q'):
                    logger.info("User requested exit")
                    running = False
                    break
                elif key == ord('s'):
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"screenshot_{timestamp}.jpg"
                    cv2.imwrite(filename, processed_frame)
                    logger.info(f"Screenshot saved: {filename}")
                elif key == ord('r'):
                    # Reset all daily timers
                    for tracker in detector.area_trackers.values():
                        tracker.reset_daily_timer()
                    detector.freetime_tracker.reset_daily_timer()
                    detector.glove_tracker.reset_daily_timer()
                    logger.info("All daily timers reset")
            else:
                time.sleep(0.033)  # Control frame rate untuk headless mode
            
        except Exception as e:
            logger.error(f"Display error: {e}")
            time.sleep(1)
    
    logger.info("Display thread stopping")
    if show_window:
        cv2.destroyAllWindows()

def main():
    """Enhanced main function"""
    global running
    setup_signal_handlers()
    
    try:
        logger.info("=== Starting Enhanced Material Dipping Detection System V4 ===")
        
        # Database configuration
        db_config = {
            'host': 'host',           # ganti dengan host yang sesuai
            'database': 'database',   # ganti dengan database yang sesuai
            'user': 'user',           # ganti dengan user yang sesuai
            'password': 'password'    # ganti dengan password yang sesuai
        }
        
        # Model path
        model_path = 'path\to\your_model.pt' # ubah sesuai path model YOLO yang digunakan
        
        # RTSP configuration
        rtsp_config = {
            'ip': 'ip',              # sesuaikan dengan IP CCTV
            'port': 'port',          # sesuaikan dengan port CCTV
            'username': 'username',  # sesuaikan dengan username login CCTV
            'password': 'password'   # sesuaikan dengan password login CCTV
        }
        
        # Choose source
        use_rtsp = True
        
        if use_rtsp:
            video_source = create_rtsp_url_robust(**rtsp_config)
            logger.info(f"Using RTSP stream: {rtsp_config['ip']}:{rtsp_config['port']}")
        else:
            video_source = "path/to/video.mp4"   # ubah sesuai path video jika menggunakan rekaman (bukan stream)
            logger.info(f"Using video file: {video_source}")
        
        # Initialize detector
        detector = EnhancedMaterialDippingDetection(
            model_path=model_path,
            video_source=video_source,
            db_config=db_config,
            is_rtsp=use_rtsp,
            rtsp_config=rtsp_config if use_rtsp else None,
            area_name_db="nama_area"
        )
        
        # Setup streaming server
        streaming_port = 8080
        streaming_server = StreamingServer(('your_local_ip', streaming_port), StreamingHandler)
        streaming_thread = threading.Thread(
            target=streaming_server.serve_forever, 
            name="StreamingThread"
        )
        streaming_thread.daemon = True

        # Setup threads
        threads = []
        
        receive_thread = threading.Thread(
            target=Receive, 
            args=(video_source, frame_queue), 
            name="ReceiveThread"
        )
        
        display_thread = threading.Thread(
            target=Display, 
            args=(detector, frame_queue, streaming_server, "Enhanced Material Dipping Detection V4"), 
            name="DisplayThread"
        )
        
        for thread in [receive_thread, display_thread, streaming_thread]:
            thread.daemon = True
            threads.append(thread)
        
        # Start threads
        for thread in threads:
            thread.start()
            logger.info(f"Started {thread.name}")
        
        logger.info("All threads started successfully")
        logger.info(f"Streaming server started at: http://localhost:{streaming_port}/stream1.mjpg")
        logger.info("Controls:")
        logger.info("- Press 'q' to quit")
        logger.info("- Press 's' to take screenshot")
        logger.info("- Press 'r' to reset all daily timers")
        logger.info(f"- Access stream at: http://YOUR_IP:{streaming_port}/stream1.mjpg")
        
        # Monitor threads
        while running:
            for thread in threads:
                if not thread.is_alive() and running:
                    logger.warning(f"Thread {thread.name} died")
                    running = False
                    break
            time.sleep(5)
        
        logger.info("Main program exiting")
        
        for thread in threads:
            if thread.is_alive():
                thread.join(timeout=5.0)
        
    except KeyboardInterrupt:
        logger.info("Program interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        logger.error(traceback.format_exc())
    finally:
        running = False
        watchdog.is_monitoring = False
        
        if 'detector' in locals():
            detector.cleanup()
            
            # Summary
            logger.info("\n=== SESSION SUMMARY ===")
            total_daily = sum(tracker.get_daily_total_duration() for tracker in detector.area_trackers.values())
            logger.info(f"Total Daily Time: {detector.format_duration(total_daily)}")
            
            for area_name, tracker in detector.area_trackers.items():
                daily_time = tracker.get_daily_total_duration()
                logger.info(f"{area_name}: {tracker.daily_timer.format_duration(daily_time)}")
            
            freetime_daily = detector.freetime_tracker.get_daily_total_duration()
            logger.info(f"Freetime: {detector.freetime_tracker.daily_timer.format_duration(freetime_daily)}")
            
            glove_daily = detector.glove_tracker.get_daily_total_duration()
            logger.info(f"Glove Usage: {detector.glove_tracker.daily_timer.format_duration(glove_daily)}")
        
        cv2.destroyAllWindows()
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
        gc.collect()
        
        logger.info("=== Enhanced Material Dipping Detection System V4 Stopped ===")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical(f"Fatal error: {e}")
        logger.critical(traceback.format_exc())
        sys.exit(1)
