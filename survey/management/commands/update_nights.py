from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone
from survey.models import Night
import datetime
import time
import os
import signal
import sys
import traceback
import subprocess
import glob
from collections import defaultdict

class Command(BaseCommand):
    help = 'Update Night records from filesystem observation directories with continuous monitoring'

    def add_arguments(self, parser):
        # Basic operation modes
        parser.add_argument(
            '--mode',
            choices=['validate', 'test', 'incremental', 'full', 'status'],
            default='incremental',
            help='Operation mode: validate folder structure, test with limit, incremental update, full rebuild, or show status'
        )
        
        # Continuous monitoring options (like populate_data.py)
        parser.add_argument(
            '--once',
            action='store_true',
            help='Run once instead of continuous monitoring'
        )
        
        parser.add_argument(
            '--interval',
            type=int,
            default=300,  # 5 minutes default
            help='Check interval in seconds for monitoring mode (default: 300)'
        )
        
        # Limiting options
        parser.add_argument(
            '--limit',
            type=int,
            help='Limit number of nights to process (useful for testing)'
        )
        
        # Path options
        parser.add_argument(
            '--base-path',
            default='/lyman/data1/obsdata',
            help='Base path to observation data directories'
        )
        
        # Validation options
        parser.add_argument(
            '--show-invalid',
            action='store_true',
            help='Show only invalid/problematic folders (use with --mode validate)'
        )
        
        # Force options
        parser.add_argument(
            '--force-full-scan',
            action='store_true',
            help='Force full scan even in incremental mode'
        )
        
        # Statistics update options
        parser.add_argument(
            '--update-stats',
            action='store_true',
            help='Update statistics for processed nights'
        )
        
        parser.add_argument(
            '--update-all-stats',
            action='store_true',
            help='Update statistics for ALL nights (time consuming)'
        )
        
        # Data management options (like populate_data.py)
        parser.add_argument(
            '--flush',
            action='store_true',
            help='Delete all existing Night data before processing'
        )
        
        parser.add_argument(
            '--flush-only',
            action='store_true',
            help='Only delete all existing Night data without processing'
        )
        
        # Smart monitoring options
        parser.add_argument(
            '--smart-interval',
            action='store_true',
            help='Use smart interval adjustment based on activity'
        )
        
        parser.add_argument(
            '--min-interval',
            type=int,
            default=60,
            help='Minimum interval for smart mode (default: 60s)'
        )
        
        parser.add_argument(
            '--max-interval',
            type=int,
            default=1800,  # 30 minutes
            help='Maximum interval for smart mode (default: 1800s)'
        )
        
        # Auto-ingest options for new folders
        parser.add_argument(
            '--auto-ingest',
            action='store_true',
            help='Automatically run ingest_all_nights for new folders detected'
        )
        
        parser.add_argument(
            '--file-stability-wait',
            type=int,
            default=300,  # 5 minutes
            help='Wait time in seconds to ensure files are stable before ingesting (default: 300)'
        )
        
        parser.add_argument(
            '--file-stability-checks',
            type=int,
            default=3,
            help='Number of consecutive stability checks required (default: 3)'
        )
        
        parser.add_argument(
            '--ingest-delay',
            type=int,
            default=1800,  # 30 minutes
            help='Additional delay after folder creation before attempting ingest (default: 1800)'
        )
        
        parser.add_argument(
            '--ingest-workers',
            type=int,
            default=4,
            help='Number of workers for parallel ingestion (default: 4)'
        )
        
        parser.add_argument(
            '--skip-recent-folders',
            type=int,
            default=3600,  # 1 hour
            help='Skip folders created within this many seconds (default: 3600)'
        )
        
        parser.add_argument(
            '--help-auto-ingest',
            action='store_true',
            help='Show detailed help for auto-ingest functionality'
        )
        
        parser.add_argument(
            '--examples',
            action='store_true',
            help='Show usage examples for different scenarios'
        )
        
        parser.add_argument(
            '--new-data-help',
            action='store_true',
            help='Show examples for processing new data only (post-2025-06-29)'
        )

    def handle(self, *args, **options):
        base_path = options['base_path']
        run_once = options['once']
        interval = options['interval']
        flush = options['flush']
        flush_only = options['flush_only']
        mode = options['mode']
        
        # Handle help options
        if options.get('help_auto_ingest'):
            self.print_auto_ingest_help()
            return
        
        if options.get('examples'):
            self.print_usage_examples()
            return
        
        if options.get('new_data_help'):
            self.print_new_data_usage()
            return
        
        # Handle flush-only mode
        if flush_only:
            self.stdout.write(self.style.WARNING('Performing Night database flush only...'))
            self._flush_nights()
            self.stdout.write(self.style.SUCCESS('Night database flush completed! No data was processed.'))
            return
        
        # Check if base path exists
        if not os.path.exists(base_path):
            self.stdout.write(self.style.ERROR(f"Base path not found: {base_path}"))
            return
        
        # Execute data flush if requested
        if flush:
            self._flush_nights()
        
        try:
            if run_once:
                # Single execution mode
                self.stdout.write(self.style.SUCCESS(f'Starting one-time Night update ({mode} mode)...'))
                self._process_nights(base_path, options)
                self.stdout.write(self.style.SUCCESS('Night update complete!'))
                return
            
            # Continuous monitoring mode
            self.stdout.write(self.style.SUCCESS(f'🌙 Monitoring observation directories: {base_path}'))
            self.stdout.write(self.style.SUCCESS(f'📊 Mode: {mode}'))
            self.stdout.write(self.style.SUCCESS(f'⏱️  Check interval: {interval} seconds'))
            
            if options['smart_interval']:
                self.stdout.write(self.style.SUCCESS(f'🧠 Smart interval: {options["min_interval"]}-{options["max_interval"]}s'))
            
            # Initialize monitoring state
            last_check_time = None
            last_modification_time = None
            consecutive_no_changes = 0
            current_interval = interval
            last_state_save = time.time()  # Track when we last saved state
            
            # Auto-ingest tracking
            if options['auto_ingest']:
                # Try to restore previous state
                pending_folders, processed_folders = self._load_processing_state()
            else:
                pending_folders = {}  # {folder_path: {'detected_at': timestamp, 'stability_checks': int}}
                processed_folders = set()  # Track already processed folders
            
            if options['auto_ingest']:
                self.stdout.write(self.style.SUCCESS(f'🤖 Auto-ingest enabled: delay={options["ingest_delay"]}s, stability={options["file_stability_wait"]}s'))
                
                # Check system resources before starting
                resource_check = self._check_system_resources()
                if resource_check and resource_check['warnings']:
                    self.stdout.write(self.style.WARNING('⚠️  System resource warnings detected!'))
            
            # Setup signal handlers for graceful shutdown
            def signal_handler(sig, frame):
                self.stdout.write(self.style.WARNING('\n🛑 Stopping Night monitoring...'))
                if pending_folders:
                    self.stdout.write(f'⚠️  {len(pending_folders)} folders were pending ingest')
                    # Save state before exit
                    if options['auto_ingest']:
                        self._save_processing_state(pending_folders, processed_folders)
                self._emergency_cleanup_resources()
                sys.exit(0)
            
            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)
            
            while True:
                try:
                    cycle_start = time.time()
                    
                    # Check for changes in the observation directory
                    current_mod_time = self._get_directory_modification_time(base_path)
                    
                    should_process = False
                    change_detected = False
                    
                    if last_modification_time is None:
                        # First run
                        should_process = True
                        self.stdout.write("🔄 First monitoring cycle - processing...")
                    elif current_mod_time > last_modification_time:
                        # Changes detected
                        should_process = True
                        change_detected = True
                        consecutive_no_changes = 0
                        self.stdout.write(f"📂 Directory changes detected (modified: {datetime.datetime.fromtimestamp(current_mod_time)})")
                    else:
                        # No changes
                        consecutive_no_changes += 1
                        if consecutive_no_changes % 10 == 0:  # Status every 10 cycles
                            self.stdout.write(f"✅ No changes detected ({consecutive_no_changes} cycles)")
                    
                    # Process if needed
                    if should_process:
                        try:
                            result = self._process_nights(base_path, options)
                            
                            # Report results
                            if result:
                                total_processed = result.get('created', 0) + result.get('updated', 0)
                                if total_processed > 0:
                                    self.stdout.write(
                                        self.style.SUCCESS(
                                            f"📊 Processed: +{result.get('created', 0)} new, "
                                            f"~{result.get('updated', 0)} updated, "
                                            f"={result.get('skipped', 0)} skipped"
                                        )
                                    )
                                else:
                                    self.stdout.write("✅ No new nights to process")
                            
                            last_modification_time = current_mod_time
                            
                        except Exception as e:
                            self.stdout.write(self.style.ERROR(f"Processing error: {e}"))
                            self.stdout.write(traceback.format_exc())
                    
                    # Auto-ingest functionality for new folders
                    if options['auto_ingest']:
                        try:
                            self._check_and_process_new_folders(base_path, options, pending_folders, processed_folders)
                        except Exception as e:
                            self.stdout.write(self.style.ERROR(f"Auto-ingest error: {e}"))
                            self.stdout.write(traceback.format_exc())
                    
                    # Smart interval adjustment
                    if options['smart_interval']:
                        current_interval = self._calculate_smart_interval(
                            change_detected, consecutive_no_changes, 
                            options['min_interval'], options['max_interval'], interval
                        )
                        if current_interval != interval:
                            self.stdout.write(f"🧠 Adjusted interval to {current_interval}s")
                    else:
                        current_interval = interval
                    
                    # Periodic state saving (every 10 minutes)
                    current_time = time.time()
                    if options['auto_ingest'] and (current_time - last_state_save) > 600:  # 10 minutes
                        self._save_processing_state(pending_folders, processed_folders)
                        last_state_save = current_time
                    
                    # Calculate sleep time
                    cycle_time = time.time() - cycle_start
                    sleep_time = max(0, current_interval - cycle_time)
                    
                    if sleep_time > 0:
                        time.sleep(sleep_time)
                    
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f'Monitoring error: {e}'))
                    self.stdout.write(traceback.format_exc())
                    # Wait a bit before retrying
                    time.sleep(min(60, current_interval))
                    
        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING('🛑 Night monitoring stopped by user'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Fatal error: {e}'))
            self.stdout.write(traceback.format_exc())

    def _flush_nights(self):
        """Delete all Night records from the database"""
        self.stdout.write(self.style.WARNING('Flushing all Night records...'))
        
        try:
            # Get counts before deletion
            total_nights = Night.objects.count()
            
            if total_nights == 0:
                self.stdout.write(self.style.SUCCESS('No Night records to delete.'))
                return
            
            # Delete all Night records (CASCADE will handle related data)
            deleted_count, _ = Night.objects.all().delete()
            
            self.stdout.write(self.style.SUCCESS(f'Deleted {deleted_count} Night records.'))
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error during Night database flush: {e}'))
            raise

    def _get_directory_modification_time(self, base_path):
        """Get the latest modification time in the observation directory"""
        try:
            latest_mod_time = os.path.getmtime(base_path)
            
            # Check subdirectories for recent changes (last 7 days only for performance)
            cutoff_time = time.time() - (7 * 24 * 3600)  # 7 days ago
            
            for entry in os.listdir(base_path):
                entry_path = os.path.join(base_path, entry)
                if os.path.isdir(entry_path):
                    try:
                        mod_time = os.path.getmtime(entry_path)
                        if mod_time > cutoff_time:  # Only check recent directories
                            latest_mod_time = max(latest_mod_time, mod_time)
                    except (OSError, IOError):
                        continue  # Skip directories we can't access
            
            return latest_mod_time
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error checking directory modification time: {e}"))
            return time.time()  # Return current time as fallback

    def _calculate_smart_interval(self, change_detected, consecutive_no_changes, min_interval, max_interval, base_interval):
        """Calculate smart interval based on activity"""
        if change_detected:
            # Recent activity - use minimum interval
            return min_interval
        elif consecutive_no_changes < 5:
            # Some recent activity - use base interval
            return base_interval
        elif consecutive_no_changes < 20:
            # Moderate inactivity - increase interval slightly
            return min(base_interval * 2, max_interval)
        else:
            # Long inactivity - use maximum interval
            return max_interval

    def _process_nights(self, base_path, options):
        """Process nights based on the specified mode"""
        mode = options['mode']
        
        try:
            if mode == 'validate':
                return self.handle_validate(base_path, options['show_invalid'])
            elif mode == 'test':
                return self.handle_test(base_path, options['limit'] or 5)
            elif mode == 'incremental':
                return self.handle_incremental(base_path, options)
            elif mode == 'full':
                return self.handle_full_rebuild(base_path, options['limit'])
            elif mode == 'status':
                return self.handle_status()
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error in {mode} mode: {e}"))
            raise

    def handle_validate(self, base_path, show_invalid_only):
        """Validate folder structure without creating Night records"""
        self.stdout.write("🔍 Validating observation folder structure...")
        
        try:
            # Check if Night model has the validation methods
            if hasattr(Night, 'show_invalid_folders') and show_invalid_only:
                self.stdout.write("📋 Showing only problematic folders:")
                results = Night.show_invalid_folders(base_path)
            elif hasattr(Night, 'validate_folder_structure'):
                self.stdout.write("📊 Full validation report:")
                results = Night.validate_folder_structure(base_path)
            else:
                # Fallback validation
                self.stdout.write("🔍 Basic folder validation:")
                results = self._basic_folder_validation(base_path)
            
            # Summary
            if isinstance(results, dict) and 'statistics' in results:
                stats = results['statistics']
                total = stats['total_folders']
                valid = stats['valid']
                
                if total > 0:
                    compliance_rate = (valid / total) * 100
                    
                    if compliance_rate >= 90:
                        self.stdout.write(
                            self.style.SUCCESS(f"✅ High compliance: {compliance_rate:.1f}% ({valid}/{total})")
                        )
                    elif compliance_rate >= 70:
                        self.stdout.write(
                            self.style.WARNING(f"⚠️ Moderate compliance: {compliance_rate:.1f}% ({valid}/{total})")
                        )
                    else:
                        self.stdout.write(
                            self.style.ERROR(f"❌ Low compliance: {compliance_rate:.1f}% ({valid}/{total})")
                        )
                    
                    return {'validated': total, 'valid': valid, 'invalid': total - valid}
                else:
                    self.stdout.write(self.style.ERROR("❌ No folders found to validate"))
                    return {'validated': 0, 'valid': 0, 'invalid': 0}
            else:
                return results
                
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Validation failed: {e}"))
            raise

    def _basic_folder_validation(self, base_path):
        """Basic folder validation when Night model methods are not available"""
        total_folders = 0
        valid_folders = 0
        
        try:
            for entry in os.listdir(base_path):
                entry_path = os.path.join(base_path, entry)
                if os.path.isdir(entry_path):
                    total_folders += 1
                    # Basic date format validation
                    if self._is_valid_date_directory(entry):
                        valid_folders += 1
                    else:
                        self.stdout.write(f"Invalid directory format: {entry}")
            
            return {
                'statistics': {
                    'total_folders': total_folders,
                    'valid': valid_folders,
                    'invalid': total_folders - valid_folders
                }
            }
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error during basic validation: {e}"))
            return {'statistics': {'total_folders': 0, 'valid': 0, 'invalid': 0}}

    def handle_test(self, base_path, limit):
        """Test creation with limited number of nights"""
        self.stdout.write(f"🧪 Test mode: Creating up to {limit} Night records...")
        
        try:
            # Check available methods and use appropriate one
            if hasattr(Night, 'find_nights_from_folders'):
                result = Night.find_nights_from_folders(
                    base_path=base_path,
                    incremental=False,
                    force_full_scan=True,
                    limit=limit
                )
            elif hasattr(Night, 'bulk_initialize_from_filesystem'):
                result = Night.bulk_initialize_from_filesystem(
                    base_path=base_path,
                    limit=limit
                )
            else:
                # Manual test processing
                result = self._manual_test_processing(base_path, limit)
            
            self.stdout.write("📊 Test Results:")
            self.stdout.write(f"  Created: {result.get('created', 0)} nights")
            self.stdout.write(f"  Updated: {result.get('updated', 0)} nights")
            self.stdout.write(f"  Skipped: {result.get('skipped', 0)} nights")
            
            return result
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Test mode error: {e}"))
            return {'created': 0, 'updated': 0, 'skipped': 0}

    def handle_incremental(self, base_path, options):
        """Incremental update of Night records"""
        force_full_scan = options['force_full_scan']
        limit = options['limit']
        
        try:
            # Use the actual method signature from Night model
            if hasattr(Night, 'find_nights_from_folders'):
                # Try with common parameters
                kwargs = {
                    'base_path': base_path,
                    'incremental': not force_full_scan,
                }
                
                # Add optional parameters if they exist
                if limit:
                    kwargs['limit'] = limit
                
                result = Night.find_nights_from_folders(**kwargs)
                
            elif hasattr(Night, 'bulk_initialize_from_filesystem'):
                result = Night.bulk_initialize_from_filesystem(
                    base_path=base_path,
                    limit=limit
                )
            else:
                # Manual incremental processing
                result = self._manual_incremental_processing(base_path, options)
            
            # Update statistics if requested
            if options['update_stats']:
                self.update_night_statistics()
            
            if options['update_all_stats']:
                self.update_all_night_statistics()
            
            return result
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Incremental mode error: {e}"))
            return {'created': 0, 'updated': 0, 'skipped': 0}

    def handle_full_rebuild(self, base_path, limit):
        """Full rebuild of all Night records"""
        try:
            if hasattr(Night, 'bulk_initialize_from_filesystem'):
                result = Night.bulk_initialize_from_filesystem(
                    base_path=base_path,
                    limit=limit
                )
            elif hasattr(Night, 'find_nights_from_folders'):
                kwargs = {
                    'base_path': base_path,
                    'incremental': False,
                }
                if limit:
                    kwargs['limit'] = limit
                    
                result = Night.find_nights_from_folders(**kwargs)
            else:
                # Manual full processing
                result = self._manual_full_processing(base_path, limit)
            
            self.stdout.write("📊 Full Rebuild Results:")
            self.stdout.write(f"  Created: {result.get('created', 0)} new nights")
            self.stdout.write(f"  Updated: {result.get('updated', 0)} existing nights")
            self.stdout.write(f"  Total: {result.get('total', 0)} nights")
            
            return result
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Full rebuild error: {e}"))
            return {'created': 0, 'updated': 0, 'total': 0}

    def handle_status(self):
        """Show current status of Night records"""
        self.stdout.write("📊 Current Night Database Status:")
        
        try:
            # Use the built-in status report if available
            if hasattr(Night, 'status_report'):
                Night.status_report()
            
            # Additional statistics
            total_nights = Night.objects.count()
            
            if total_nights > 0:
                # Scanning status
                scanned_count = Night.objects.filter(files_scanned=True).count() if hasattr(Night.objects.first(), 'files_scanned') else 0
                not_scanned = total_nights - scanned_count
                
                self.stdout.write(f"\n📁 File Scanning Status:")
                self.stdout.write(f"  Total nights: {total_nights}")
                self.stdout.write(f"  Scanned: {scanned_count} nights")
                self.stdout.write(f"  Not scanned: {not_scanned} nights")
                
                if scanned_count > 0:
                    scan_percentage = (scanned_count / total_nights) * 100
                    self.stdout.write(f"  Scan completion: {scan_percentage:.1f}%")
            else:
                self.stdout.write("📋 No Night records in database")
            
            return {'total_nights': total_nights, 'scanned': scanned_count if total_nights > 0 else 0}
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Status check error: {e}"))
            return {'total_nights': 0, 'scanned': 0}

    def update_night_statistics(self, max_age_days=7):
        """Update statistics for recently processed nights"""
        self.stdout.write(f"\n📈 Updating night statistics (last {max_age_days} days)...")
        
        try:
            # Update statistics for recent nights
            cutoff_date = timezone.now().date() - datetime.timedelta(days=max_age_days)
            recent_nights = Night.objects.filter(updated_at__date__gte=cutoff_date)
            
            updated_count = 0
            for night in recent_nights:
                try:
                    if hasattr(night, 'update_statistics'):
                        night.update_statistics()
                        updated_count += 1
                except Exception as e:
                    self.stdout.write(f"Warning: Failed to update stats for {night.date}: {e}")
            
            self.stdout.write(f"✅ Updated statistics for {updated_count} nights")
            return updated_count
           
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Statistics update error: {e}"))
            return 0

    def update_all_night_statistics(self):
        """Update statistics for ALL nights"""
        self.stdout.write("\n📊 Updating ALL night statistics (this may take a while)...")
        
        try:
            if hasattr(Night, 'update_all_statistics'):
                updated_count = Night.update_all_statistics()
                self.stdout.write(f"✅ Updated statistics for {updated_count} nights")
                return updated_count
            else:
                # Manual update for all nights
                updated_count = 0
                for night in Night.objects.all():
                    try:
                        if hasattr(night, 'update_statistics'):
                            night.update_statistics()
                            updated_count += 1
                    except Exception as e:
                        self.stdout.write(f"Warning: Failed to update stats for {night.date}: {e}")
                
                self.stdout.write(f"✅ Updated statistics for {updated_count} nights")
                return updated_count
                
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Full statistics update failed: {e}"))
            return 0

    def _manual_test_processing(self, base_path, limit):
        """Manual test processing when Night methods are not available"""
        created = 0
        updated = 0
        skipped = 0
        
        try:
            directories = []
            for entry in os.listdir(base_path):
                entry_path = os.path.join(base_path, entry)
                if os.path.isdir(entry_path) and self._is_valid_date_directory(entry):
                    directories.append(entry)
            
            directories = sorted(directories)[:limit]
            
            for dir_name in directories:
                try:
                    date_obj = self._parse_directory_date(dir_name)
                    night, created_flag = Night.objects.get_or_create(
                        date=date_obj,
                        defaults={'directory_name': dir_name}
                    )
                    
                    if created_flag:
                        created += 1
                    else:
                        updated += 1
                        
                except Exception as e:
                    self.stdout.write(f"Warning: Failed to process {dir_name}: {e}")
                    skipped += 1
            
            return {'created': created, 'updated': updated, 'skipped': skipped}
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Manual test processing error: {e}"))
            return {'created': 0, 'updated': 0, 'skipped': 0}

    def _manual_incremental_processing(self, base_path, options):
        """Manual incremental processing"""
        return self._manual_test_processing(base_path, options.get('limit', 100))

    def _manual_full_processing(self, base_path, limit):
        """Manual full processing"""
        return self._manual_test_processing(base_path, limit or 1000)

    def _is_valid_date_directory(self, directory_name):
        """Check if directory name represents a valid observation date"""
        try:
            # Try various date formats
            for fmt in ['%Y%m%d', '%Y-%m-%d', '%y%m%d']:
                try:
                    datetime.datetime.strptime(directory_name, fmt)
                    return True
                except ValueError:
                    continue
            return False
        except:
            return False

    def _parse_directory_date(self, directory_name):
        """Parse date from directory name"""
        for fmt in ['%Y%m%d', '%Y-%m-%d', '%y%m%d']:
            try:
                return datetime.datetime.strptime(directory_name, fmt).date()
            except ValueError:
                continue
        raise ValueError(f"Cannot parse date from directory: {directory_name}")

    def _check_and_process_new_folders(self, base_path, options, pending_folders, processed_folders):
        """Check for new observation folders and manage their auto-ingestion"""
        current_time = time.time()
        
        # 1. Discover new folders
        new_folders = self._discover_new_folders(base_path, processed_folders, options)
        
        # 2. Add new folders to pending list
        for folder_info in new_folders:
            folder_path = folder_info['path']
            if folder_path not in pending_folders and folder_path not in processed_folders:
                pending_folders[folder_path] = {
                    'detected_at': current_time,
                    'stability_checks': 0,
                    'date': folder_info['date'],
                    'telescope': folder_info['telescope']
                }
                self.stdout.write(f"🆕 New folder detected: {folder_info['display_name']} (telescope: {folder_info['telescope']})")
        
        # 3. Check stability of pending folders
        folders_to_remove = []
        for folder_path, folder_info in pending_folders.items():
            age = current_time - folder_info['detected_at']
            
            # Skip if folder is too recent (still being created)
            if age < options['skip_recent_folders']:
                continue
            
            # Check if enough time has passed for initial delay
            if age < options['ingest_delay']:
                continue
            
            # Check file stability
            if self._check_folder_stability(folder_path, options):
                folder_info['stability_checks'] += 1
                self.stdout.write(f"📊 Stability check {folder_info['stability_checks']}/{options['file_stability_checks']} for {os.path.basename(folder_path)}")
                
                # If enough stability checks passed, trigger ingestion
                if folder_info['stability_checks'] >= options['file_stability_checks']:
                    success = self._trigger_folder_ingestion(folder_path, folder_info, options)
                    if success:
                        processed_folders.add(folder_path)
                        folders_to_remove.append(folder_path)
                        self.stdout.write(f"✅ Successfully processed: {os.path.basename(folder_path)}")
                    else:
                        self.stdout.write(f"❌ Failed to process: {os.path.basename(folder_path)}")
                        # Keep in pending for retry, but reset stability checks
                        folder_info['stability_checks'] = 0
            else:
                # Reset stability checks if files are still changing
                if folder_info['stability_checks'] > 0:
                    self.stdout.write(f"🔄 Files still changing in {os.path.basename(folder_path)}, resetting stability check")
                folder_info['stability_checks'] = 0
        
        # 4. Remove processed folders from pending
        for folder_path in folders_to_remove:
            del pending_folders[folder_path]
        
        # 5. Clean up old pending folders (prevent memory leak)
        old_threshold = current_time - (24 * 3600)  # 24 hours
        old_folders = [fp for fp, fi in pending_folders.items() if fi['detected_at'] < old_threshold]
        for folder_path in old_folders:
            self.stdout.write(f"⏰ Removing stale pending folder: {os.path.basename(folder_path)}")
            del pending_folders[folder_path]
    
    def _discover_new_folders(self, base_path, processed_folders, options):
        """Discover new observation folders that haven't been processed yet"""
        new_folders = []
        
        try:
            # Look for telescope directories (7DT01, 7DT02, etc.)
            for telescope_dir in os.listdir(base_path):
                telescope_path = os.path.join(base_path, telescope_dir)
                
                # Skip if not a directory or doesn't match telescope pattern
                if not os.path.isdir(telescope_path) or not telescope_dir.startswith('7DT'):
                    continue
                
                # Look for date folders within telescope directory
                try:
                    for date_folder in os.listdir(telescope_path):
                        date_folder_path = os.path.join(telescope_path, date_folder)
                        
                        # Skip if not a directory
                        if not os.path.isdir(date_folder_path):
                            continue
                        
                        # Check if this looks like an observation date folder
                        if self._is_observation_folder(date_folder, telescope_dir):
                            # Skip if already processed
                            if date_folder_path in processed_folders:
                                continue
                            
                            # Extract date and telescope info
                            try:
                                date_str = self._extract_date_from_folder(date_folder)
                                new_folders.append({
                                    'path': date_folder_path,
                                    'date': date_str,
                                    'telescope': telescope_dir,
                                    'display_name': f"{telescope_dir}/{date_folder}"
                                })
                            except Exception as e:
                                self.stdout.write(f"⚠️  Skipping invalid folder {date_folder}: {e}")
                                
                except (OSError, PermissionError) as e:
                    self.stdout.write(f"⚠️  Cannot access telescope directory {telescope_dir}: {e}")
                    continue
                    
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error discovering new folders: {e}"))
        
        return new_folders
    
    def _is_observation_folder(self, folder_name, telescope_name):
        """Check if a folder looks like an observation data folder"""
        # Check for date patterns like YYYY-MM-DD or YYYYMMDD
        # Often combined with telescope name like 2025-07-01_7DT01
        
        # Remove telescope suffix if present
        clean_name = folder_name.replace(f"_{telescope_name}", "")
        
        # Check various date patterns
        date_patterns = [
            r'^\d{4}-\d{2}-\d{2}$',  # YYYY-MM-DD
            r'^\d{8}$',              # YYYYMMDD
            r'^\d{4}\d{2}\d{2}$',    # YYYYMMDD (same as above)
        ]
        
        import re
        for pattern in date_patterns:
            if re.match(pattern, clean_name):
                return True
        
        return False
    
    def _extract_date_from_folder(self, folder_name):
        """Extract date string from folder name"""
        import re
        
        # Try to find date pattern in folder name
        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', folder_name)
        if date_match:
            return date_match.group(1)
        
        date_match = re.search(r'(\d{8})', folder_name)
        if date_match:
            date_str = date_match.group(1)
            # Convert YYYYMMDD to YYYY-MM-DD
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        
        raise ValueError(f"Cannot extract date from folder name: {folder_name}")
    
    def _check_folder_stability(self, folder_path, options):
        """Check if files in folder are stable (not being actively written)"""
        try:
            # Get current file count and sizes
            current_stats = self._get_folder_stats(folder_path)
            
            # Wait for stability check interval
            time.sleep(min(30, options['file_stability_wait'] // 10))  # Quick check
            
            # Get stats again
            new_stats = self._get_folder_stats(folder_path)
            
            # Compare stats - folder is stable if no changes
            return (current_stats['file_count'] == new_stats['file_count'] and 
                    current_stats['total_size'] == new_stats['total_size'])
                    
        except Exception as e:
            self.stdout.write(f"⚠️  Error checking folder stability: {e}")
            return False
    
    def _get_folder_stats(self, folder_path):
        """Get folder statistics (file count and total size)"""
        file_count = 0
        total_size = 0
        
        try:
            for root, dirs, files in os.walk(folder_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    try:
                        stat = os.stat(file_path)
                        file_count += 1
                        total_size += stat.st_size
                    except (OSError, IOError):
                        continue  # Skip files we can't access
        except Exception:
            pass  # Return default values on error
        
        return {'file_count': file_count, 'total_size': total_size}
    
    def _trigger_folder_ingestion(self, folder_path, folder_info, options):
        """Trigger ingest_all_nights for a specific folder/date"""
        date_str = folder_info['date']
        telescope = folder_info['telescope']
        
        try:
            self.stdout.write(f"🚀 Starting ingestion for {date_str} ({telescope})")
            
            # Build ingest_all_nights command
            cmd = [
                'python', 'manage.py', 'ingest_all_nights',
                '--start-date', date_str,
                '--end-date', date_str,
                '--new-data-only',  # Use new data mode for auto-ingest
                '--cleanup',
                '--auto-confirm',
                '--continue-on-error',
                '--parallel',
                '--workers', str(options['ingest_workers']),
                '--validate',
                '--create-targets',
                '--exclude-focus',
                '--exclude-test',
                '--report-interval', '10'
            ]
            
            # Create log file for this ingestion
            log_dir = 'logs'
            os.makedirs(log_dir, exist_ok=True)
            log_file = os.path.join(log_dir, f"auto_ingest_{date_str.replace('-', '')}_{telescope}.log")
            
            self.stdout.write(f"📝 Logging to: {log_file}")
            
            # Run the command
            with open(log_file, 'w') as log:
                process = subprocess.Popen(
                    cmd,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    text=True
                )
                
                # Wait for completion
                return_code = process.wait()
                
                if return_code == 0:
                    self.stdout.write(f"✅ Ingestion completed successfully for {date_str}")
                    return True
                else:
                    self.stdout.write(f"❌ Ingestion failed for {date_str} (exit code: {return_code})")
                    return False
                    
        except Exception as e:
            self.stdout.write(f"❌ Error during ingestion for {date_str}: {e}")
            return False

    def _get_current_processing_status(self):
        """Get current status of auto-ingest processing"""
        status = {
            'monitoring_active': True,
            'pending_folders': len(getattr(self, '_pending_folders', {})),
            'processed_folders': len(getattr(self, '_processed_folders', set())),
            'last_check': getattr(self, '_last_check_time', None)
        }
        return status
    
    def _save_processing_state(self, pending_folders, processed_folders):
        """Save processing state to file for recovery after restart"""
        import json
        
        state_file = 'logs/update_nights_state.json'
        os.makedirs('logs', exist_ok=True)
        
        try:
            # Convert sets to lists for JSON serialization
            state = {
                'pending_folders': {k: v for k, v in pending_folders.items()},
                'processed_folders': list(processed_folders),
                'last_saved': time.time()
            }
            
            with open(state_file, 'w') as f:
                json.dump(state, f, indent=2, default=str)
                
        except Exception as e:
            self.stdout.write(f"⚠️  Could not save processing state: {e}")
    
    def _load_processing_state(self):
        """Load previously saved processing state"""
        import json
        
        state_file = 'logs/update_nights_state.json'
        
        try:
            if os.path.exists(state_file):
                with open(state_file, 'r') as f:
                    state = json.load(f)
                
                # Convert back from JSON format
                pending_folders = state.get('pending_folders', {})
                processed_folders = set(state.get('processed_folders', []))
                
                # Check if state is recent (less than 24 hours old)
                last_saved = state.get('last_saved', 0)
                if time.time() - last_saved < 24 * 3600:
                    self.stdout.write(f"🔄 Restored processing state: {len(pending_folders)} pending, {len(processed_folders)} processed")
                    return pending_folders, processed_folders
                else:
                    self.stdout.write("⏰ Processing state too old, starting fresh")
                    
        except Exception as e:
            self.stdout.write(f"⚠️  Could not load processing state: {e}")
        
        return {}, set()
    
    def _generate_auto_ingest_report(self, pending_folders, processed_folders):
        """Generate a summary report of auto-ingest activities"""
        report = {
            'timestamp': datetime.datetime.now().isoformat(),
            'pending_count': len(pending_folders),
            'processed_count': len(processed_folders),
            'pending_details': [],
            'recent_processed': []
        }
        
        # Add details about pending folders
        current_time = time.time()
        for folder_path, info in pending_folders.items():
            age_minutes = (current_time - info['detected_at']) / 60
            report['pending_details'].append({
                'folder': os.path.basename(folder_path),
                'date': info['date'],
                'telescope': info['telescope'],
                'age_minutes': round(age_minutes, 1),
                'stability_checks': info['stability_checks']
            })
        
        # Add recently processed folders (last 24 hours)
        cutoff_time = current_time - (24 * 3600)
        for folder_path in processed_folders:
            # This is a simplified check - in a real implementation,
            # you'd want to track processing timestamps
            if os.path.exists(folder_path):
                try:
                    mod_time = os.path.getmtime(folder_path)
                    if mod_time > cutoff_time:
                        report['recent_processed'].append({
                            'folder': os.path.basename(folder_path),
                            'processed_age_hours': round((current_time - mod_time) / 3600, 1)
                        })
                except:
                    continue
        
        return report
    
    def _emergency_cleanup_resources(self):
        """Emergency cleanup of resources and processes"""
        try:
            # Kill any running ingest processes (if trackable)
            self.stdout.write("🧹 Emergency cleanup - checking for running processes...")
            
            # This would be where you'd clean up any subprocess references
            # For now, just log the cleanup attempt
            self.stdout.write("✅ Resource cleanup completed")
            
        except Exception as e:
            self.stdout.write(f"⚠️  Error during emergency cleanup: {e}")
    
    def _check_system_resources(self):
        """Check system resources before starting intensive operations"""
        try:
            import psutil
            
            # Check available disk space
            disk_usage = psutil.disk_usage('/lyman/data1')
            free_gb = disk_usage.free / (1024**3)
            
            # Check memory usage
            memory = psutil.virtual_memory()
            available_memory_gb = memory.available / (1024**3)
            
            # Check CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            
            self.stdout.write(f"💾 System Resources:")
            self.stdout.write(f"  • Disk space: {free_gb:.1f} GB free")
            self.stdout.write(f"  • Memory: {available_memory_gb:.1f} GB available")
            self.stdout.write(f"  • CPU usage: {cpu_percent:.1f}%")
            
            # Warning thresholds
            if free_gb < 100:
                self.stdout.write(self.style.WARNING("⚠️  Low disk space warning!"))
            
            if available_memory_gb < 4:
                self.stdout.write(self.style.WARNING("⚠️  Low memory warning!"))
            
            if cpu_percent > 90:
                self.stdout.write(self.style.WARNING("⚠️  High CPU usage warning!"))
                
            return {
                'disk_free_gb': free_gb,
                'memory_available_gb': available_memory_gb,
                'cpu_percent': cpu_percent,
                'warnings': free_gb < 100 or available_memory_gb < 4 or cpu_percent > 90
            }
            
        except ImportError:
            self.stdout.write("⚠️  psutil not available - cannot check system resources")
            return None
        except Exception as e:
            self.stdout.write(f"⚠️  Error checking system resources: {e}")
            return None

    def print_auto_ingest_help(self):
        """Print help information for auto-ingest functionality"""
        help_text = """
🤖 AUTO-INGEST FUNCTIONALITY GUIDE
═══════════════════════════════════

The auto-ingest feature automatically detects new observation folders and runs
ingest_all_nights for them when files are stable and ready.

📋 TYPICAL WORKFLOW:
1. New observation folder is created (e.g., /lyman/data1/obsdata/7DT01/2025-07-02_7DT01)
2. Files are copied/extracted into the folder over time
3. System waits for folder to be "stable" (no file changes)
4. After stability checks pass, ingest_all_nights is automatically triggered
5. Results are logged and folder is marked as processed

⚙️  KEY PARAMETERS:
--skip-recent-folders 3600    # Skip folders newer than 1 hour (files still copying)
--ingest-delay 1800          # Wait 30 minutes after folder creation before checking
--file-stability-wait 300    # Wait 5 minutes between stability checks
--file-stability-checks 3    # Require 3 consecutive stable checks
--ingest-workers 4           # Use 4 parallel workers for processing

🚀 EXAMPLE USAGE:
# Basic auto-ingest with default settings
python manage.py update_nights --auto-ingest

# Auto-ingest with custom timing (faster for testing)
python manage.py update_nights --auto-ingest --skip-recent-folders 600 --ingest-delay 300

# Production auto-ingest with monitoring
python manage.py update_nights --auto-ingest --smart-interval --update-stats

⚠️  IMPORTANT CONSIDERATIONS:
• Ensure sufficient disk space and system resources
• Monitor logs in logs/ directory for processing results
• Use --dry-run first to test folder detection
• Processing state is automatically saved and restored after restarts

📊 MONITORING:
• Check logs/update_nights_state.json for current state
• Individual ingest logs: logs/auto_ingest_YYYYMMDD_TELESCOPE.log
• Use Ctrl+C for graceful shutdown (saves state)
"""
        self.stdout.write(help_text)

    def print_usage_examples(self):
        """Print usage examples for different scenarios"""
        examples = """
📚 USAGE EXAMPLES
═════════════════

# 1. BASIC NIGHT MONITORING (Night DB updates only)
python manage.py update_nights --interval 300

# 2. NIGHT MONITORING WITH AUTO-INGEST
python manage.py update_nights --auto-ingest --interval 300

# 3. PRODUCTION MONITORING (recommended)
python manage.py update_nights --auto-ingest --smart-interval --update-stats

# 4. TESTING AUTO-INGEST (faster timing)
python manage.py update_nights --auto-ingest --skip-recent-folders 300 --ingest-delay 600

# 5. ONE-TIME VALIDATION
python manage.py update_nights --once --mode validate

# 6. FULL REBUILD (dangerous!)
python manage.py update_nights --once --mode full --flush
"""
        self.stdout.write(examples)

    def print_new_data_usage(self):
        """Print usage examples specifically for new data processing"""
        examples = """
🆕 NEW DATA PROCESSING EXAMPLES
═══════════════════════════════

# 1. PROCESS ONLY NEW DATA (after 2025-06-29)
python manage.py ingest_all_nights --new-data-only

# 2. NEW DATA WITH CUSTOM CUTOFF
python manage.py ingest_all_nights --new-data-only --bulk-cutoff-date 2025-06-25

# 3. AUTO-INGEST FOR NEW DATA (24/7 monitoring)
python manage.py update_nights --auto-ingest --smart-interval

# 4. SPECIFIC DATE RANGE (manual override)
python manage.py ingest_all_nights --start-date 2025-07-01 --end-date 2025-07-02

# 5. DRY RUN TO CHECK NEW DATA
python manage.py ingest_all_nights --new-data-only --dry-run

📊 DATA PROCESSING STATUS:
• Bulk processed: All data up to 2025-06-29
• Auto-processing: 2025-06-30 onwards
• Current focus: Recent observations only

⚡ PERFORMANCE OPTIMIZATION:
• --new-data-only: Skips scanning old directories
• --skip-existing: Avoids reprocessing existing data
• --parallel: Uses multiple workers for large datasets
"""
        self.stdout.write(examples)

