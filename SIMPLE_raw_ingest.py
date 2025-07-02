#!/usr/bin/env python3
"""
Enhanced RAW Data Ingest - Using survey.models components
Optimized for Target support with cleanup and proper calibration frame handling
"""

import os
import sys
import time
import django
from datetime import date

# Django setup
sys.path.append('/home/db/gwportal')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'gwportal.settings')
django.setup()

from survey.models import (
    Night, FrameManager, ScienceFrame, BiasFrame, DarkFrame, FlatFrame,
    Target, Tile, FilenamePatternAnalyzer, Unit, Filter
)

def cleanup_existing_data(date_str, confirm=False):
    """
    Remove all existing data for the specified date.
    
    Parameters:
    -----------
    date_str : str
        Date to clean up (YYYY-MM-DD format)
    confirm : bool
        If True, proceed without confirmation prompt
        
    Returns:
    --------
    bool : True if cleanup was performed or no data found
    """
    print("🧹 CLEANUP PHASE")
    print("-" * 40)
    
    target_date = date.fromisoformat(date_str)
    
    try:
        night = Night.objects.get(date=target_date)
        print(f"✅ Found Night record: {night}")
    except Night.DoesNotExist:
        print(f"✨ No Night record found for {date_str} - nothing to clean up")
        return True
    
    # Count frames by type
    science_count = ScienceFrame.objects.filter(night=night).count()
    bias_count = BiasFrame.objects.filter(night=night).count()
    dark_count = DarkFrame.objects.filter(night=night).count()
    flat_count = FlatFrame.objects.filter(night=night).count()
    total_frames = science_count + bias_count + dark_count + flat_count
    
    print(f"📊 Found existing data:")
    print(f"   🔬 Science frames: {science_count:,}")
    print(f"   📐 Bias frames: {bias_count:,}")
    print(f"   🌑 Dark frames: {dark_count:,}")
    print(f"   🌅 Flat frames: {flat_count:,}")
    print(f"   📊 Total frames: {total_frames:,}")
    
    # Count test targets created (BIAS, DARK, FLAT)
    test_targets = Target.objects.filter(name__in=['BIAS', 'DARK', 'FLAT'])
    test_target_count = test_targets.count()
    
    if test_target_count > 0:
        print(f"   🎯 Test targets to remove: {test_target_count}")
        for target in test_targets:
            linked_frames = target.scienceframe_set.count()
            print(f"      - {target.name}: {linked_frames} linked frames")
    
    if total_frames == 0 and test_target_count == 0:
        print("✨ No data found to clean up!")
        return True
    
    # Confirmation
    if not confirm:
        print("\n⚠️  WARNING: This will permanently delete the following:")
        if total_frames > 0:
            print(f"   • All {total_frames:,} frames for {date_str}")
        if test_target_count > 0:
            print(f"   • {test_target_count} test targets (BIAS, DARK, FLAT)")
        print()
        
        response = input("Do you want to proceed with cleanup? (type 'YES' to confirm): ")
        if response != 'YES':
            print("❌ Cleanup cancelled - stopping import process")
            return False
        print()
    
    # Delete frames
    print("🗑️  Deleting frames...")
    deleted_counts = {}
    
    for model, name in [(ScienceFrame, 'Science'), (BiasFrame, 'Bias'), 
                       (DarkFrame, 'Dark'), (FlatFrame, 'Flat')]:
        count = model.objects.filter(night=night).count()
        if count > 0:
            model.objects.filter(night=night).delete()
            deleted_counts[name] = count
            print(f"  ✅ Deleted {count:,} {name} frames")
    
    # Delete test targets
    if test_target_count > 0:
        print("🗑️  Deleting test targets...")
        for target in test_targets:
            target_name = target.name
            target.delete()
            print(f"  ✅ Deleted target: {target_name}")
    
    total_deleted = sum(deleted_counts.values())
    print(f"✅ Cleanup completed: {total_deleted:,} frames and {test_target_count} targets removed")
    print()
    
    return True


def enhanced_ingest(date_str="2025-06-04", cleanup=False, parallel=False, max_workers=4, 
                   limit=None, debug=False, validate=False, create_targets=True, 
                   exclude_focus=True, exclude_test=True, auto_confirm_cleanup=False):
    """
    Enhanced RAW data ingest with integrated cleanup and Target support.
    
    Parameters:
    -----------
    date_str : str
        Date to process in YYYY-MM-DD format
    cleanup : bool
        Remove existing data for the date before importing
    parallel : bool
        Force parallel processing (auto-enabled for 100k+ files)
    max_workers : int
        Number of parallel workers (default: 4)
    limit : int, optional
        Limit number of files to process (for testing)
    debug : bool
        Enable debug mode with detailed logging
    validate : bool
        Run validation checks after import
    create_targets : bool
        Automatically create Target objects for science frames
    exclude_focus : bool
        Exclude focus-related files from import
    exclude_test : bool
        Exclude test/calibration files from import
    auto_confirm_cleanup : bool
        Auto-confirm cleanup without user interaction
    """
    
    print("=" * 80)
    print("🚀 ENHANCED RAW DATA INGEST - Target Support Edition")
    print("=" * 80)
    print(f"📅 Target date: {date_str}")
    
    if cleanup:
        print("🧹 Cleanup mode: ENABLED")
    if create_targets:
        print("🎯 Target creation: ENABLED")
    if exclude_focus:
        print("🔍 Focus exclusion: ENABLED")
    if exclude_test:
        print("🧪 Test exclusion: ENABLED")
    
    if limit:
        print(f"🔢 File limit: {limit} files")
    if parallel:
        print(f"⚡ Parallel mode: {max_workers} workers")
    if debug:
        print("🔍 Debug mode: ENABLED")
    if validate:
        print("✅ Validation mode: ENABLED")
    
    print("=" * 80)
    
    start_total = time.time()
    
    # Step 1: Cleanup existing data if requested
    if cleanup:
        cleanup_success = cleanup_existing_data(date_str, confirm=auto_confirm_cleanup)
        if not cleanup_success:
            print("🛑 Import process stopped due to cleanup cancellation")
            return
    
    # Step 2: Initialize night object
    print("🌙 NIGHT INITIALIZATION")
    print("-" * 40)
    
    target_date = date.fromisoformat(date_str)
    night = Night.get_or_create_for_date(target_date)
    print(f"✅ Night object ready: {night}")
    print()
    
    # Step 3: Discover and filter FITS files
    print("🔍 FILE DISCOVERY & FILTERING PHASE")
    print("-" * 40)
    
    all_files = discover_fits_files(date_str, debug=debug)
    
    if not all_files:
        print("❌ No FITS files found!")
        return
    
    print(f"📊 Total files discovered: {len(all_files):,}")
    
    # Filter unwanted files if requested
    if exclude_focus or exclude_test:
        filtered_files, exclusion_stats = filter_unwanted_files(
            all_files, exclude_focus, exclude_test, debug=debug
        )
        
        print(f"🔽 Files after filtering: {len(filtered_files):,}")
        if exclusion_stats:
            print("📋 Exclusion summary:")
            for reason, count in exclusion_stats.items():
                if count > 0:
                    print(f"   {reason}: {count:,} files")
    else:
        filtered_files = all_files
        print("📋 No filtering applied - processing all files")
    
    # Apply file limit for testing
    if limit and limit < len(filtered_files):
        filtered_files = filtered_files[:limit]
        print(f"🔢 Limited to first {len(filtered_files):,} files for processing")
    
    # Auto-determine processing mode
    if len(filtered_files) >= 100000 and not parallel:
        print(f"📈 Large dataset detected ({len(filtered_files):,} files)")
        print("🚀 Auto-enabling parallel processing mode")
        parallel = True
    
    print()
    
    # Step 4: Target pre-processing (if enabled)
    if create_targets:
        print("🎯 TARGET PRE-PROCESSING PHASE")
        print("-" * 40)
        
        target_stats = pre_process_targets(filtered_files, debug=debug)
        print(f"✅ Target processing complete:")
        print(f"   🆕 New targets created: {target_stats['created']}")
        print(f"   🔄 Existing targets found: {target_stats['existing']}")
        print(f"   📊 Tiles referenced: {target_stats['tiles']}")
        if target_stats.get('calibration_skipped', 0) > 0:
            print(f"   📐 Calibration frames skipped: {target_stats['calibration_skipped']}")
        print()
    
    # Step 5: Enhanced Import using FrameManager
    print("⚡ ENHANCED IMPORT PHASE")
    print("-" * 40)
    
    start_import = time.time()
    
    # Progress tracking
    def progress_callback(processed, total, stats):
        elapsed = time.time() - start_import
        rate = processed / elapsed if elapsed > 0 else 0
        eta = (total - processed) / rate if rate > 0 else 0
        
        print(f"📈 Progress: {processed:,}/{total:,} ({processed/total*100:.1f}%) | "
              f"Rate: {rate:.1f} files/s | ETA: {eta:.0f}s")
        
        if debug and processed % 1000 == 0:
            print(f"   📊 Current stats: Imported={stats['imported']}, "
                  f"Existing={stats['existing']}, Failed={stats['failed']}")
    
    # Perform the import
    try:
        results = FrameManager.import_files(
            filtered_files, 
            night, 
            parallel=parallel,
            max_workers=max_workers,
            progress_callback=progress_callback
        )
        
        import_time = time.time() - start_import
        
    except Exception as e:
        print(f"❌ Import failed with error: {e}")
        if debug:
            import traceback
            traceback.print_exc()
        return
    
    print()
    
    # Step 6: Target post-processing (if enabled)
    if create_targets and (results['imported'] > 0 or debug):
        print("🎯 TARGET POST-PROCESSING PHASE")
        print("-" * 40)
        
        post_target_stats = post_process_targets(night, debug=debug)
        print(f"✅ Post-processing complete:")
        print(f"   🔗 Science frames linked to targets: {post_target_stats['linked_targets']}")
        print(f"   🔗 Science frames linked to tiles: {post_target_stats['linked_tiles']}")
        print(f"   📍 Coordinates updated: {post_target_stats['coordinates_updated']}")
        print()
    
    # Step 7: Report results
    print("📊 IMPORT RESULTS")
    print("-" * 40)
    
    total_time = time.time() - start_total
    
    print(f"⏱️  Total processing time: {total_time:.2f} seconds")
    print(f"⚡ Import time: {import_time:.2f} seconds")
    print(f"🚀 Overall rate: {results['total']/total_time:.1f} files/second")
    print()
    
    print("📈 Import Statistics:")
    print(f"   ✅ Successfully imported: {results['imported']:,}")
    print(f"   🔄 Already existing: {results['existing']:,}")
    print(f"   ❌ Failed: {results['failed']:,}")
    print(f"   📁 Total processed: {results['total']:,}")
    
    if results['total'] > 0:
        success_rate = (results['imported'] + results['existing']) / results['total'] * 100
        print(f"   📊 Success rate: {success_rate:.1f}%")
    print()
    
    # Frame type breakdown
    if results.get('frame_types'):
        print("🔬 Frame Type Breakdown:")
        for frame_type, count in sorted(results['frame_types'].items()):
            percentage = count / sum(results['frame_types'].values()) * 100
            print(f"   {frame_type:>8}: {count:,} ({percentage:.1f}%)")
        print()
    
    # Error reporting
    if results.get('errors'):
        print("⚠️  Error Summary:")
        print(f"   Total errors: {len(results['errors'])}")
        if debug or len(results['errors']) <= 10:
            for i, error in enumerate(results['errors'][:10], 1):
                print(f"   {i:2d}. {error}")
        print()
    
    # Step 8: Update night statistics
    print("📊 NIGHT STATISTICS UPDATE")
    print("-" * 40)
    
    try:
        night.update_statistics()
        print("✅ Night statistics updated successfully")
        print(f"   🔬 Science frames: {night.science_count:,}")
        print(f"   📐 Bias frames: {night.bias_count:,}")
        print(f"   🌑 Dark frames: {night.dark_count:,}")
        print(f"   🌅 Flat frames: {night.flat_count:,}")
        print(f"   📊 Total frames: {night.total_frames:,}")
        
    except Exception as e:
        print(f"⚠️ Could not update night statistics: {e}")
    print()
    
    # Step 9: Validation (if requested)
    if validate and results['imported'] > 0:
        print("✅ VALIDATION PHASE")
        print("-" * 40)
        
        try:
            validation_results = FrameManager.validate_imported_frames(night, sample_size=20)
            
            print(f"🔍 Validation Results:")
            print(f"   Frames checked: {validation_results['total_checked']}")
            print(f"   Headers parsed: {validation_results['header_parsed']}")
            
            if validation_results['validation_passed']:
                print("   ✅ Validation PASSED")
            else:
                print("   ⚠️ Validation issues detected")
                
        except Exception as e:
            print(f"❌ Validation failed: {e}")
        print()
    
    # Final summary
    print("🎉 ENHANCED IMPORT COMPLETED")
    print("-" * 40)
    print(f"✨ Successfully processed {results['total']:,} files in {total_time:.2f} seconds")
    
    if results['imported'] > 0:
        print(f"🎯 New frames added to database: {results['imported']:,}")
    
    if results.get('failed', 0) > 0:
        print(f"⚠️ Files that failed processing: {results['failed']:,}")
    
    print("=" * 80)


# [이전의 모든 helper 함수들을 여기에 포함]
def discover_fits_files(date_str, debug=False):
    """Discover FITS files using enhanced directory scanning."""
    all_files = []
    base_path = "/lyman/data1/obsdata"
    
    if not os.path.exists(base_path):
        print(f"❌ Base data path does not exist: {base_path}")
        return all_files
    
    print(f"🔍 Searching for FITS files in: {base_path}")
    print(f"📅 Target date: {date_str}")
    
    # Scan each telescope unit directory
    unit_dirs = []
    for item in os.listdir(base_path):
        if item.startswith('7DT') and os.path.isdir(os.path.join(base_path, item)):
            unit_dirs.append(item)
    
    unit_dirs.sort()
    print(f"🔭 Found telescope units: {unit_dirs}")
    
    for unit_dir in unit_dirs:
        unit_path = os.path.join(base_path, unit_dir)
        unit_files = []
        
        try:
            for subdir in os.listdir(unit_path):
                if subdir == date_str or subdir.startswith(f"{date_str}_"):
                    data_dir = os.path.join(unit_path, subdir)
                    if os.path.isdir(data_dir):
                        if debug:
                            print(f"  📁 Checking directory: {data_dir}")
                        
                        for root, dirs, files in os.walk(data_dir):
                            for file in files:
                                if file.endswith('.fits') or file.endswith('.fits.fz'):
                                    full_path = os.path.join(root, file)
                                    unit_files.append(full_path)
        
        except PermissionError:
            print(f"⚠️ Permission denied accessing {unit_path}")
            continue
        except Exception as e:
            print(f"⚠️ Error scanning {unit_path}: {e}")
            continue
        
        if unit_files:
            print(f"  📊 {unit_dir}: Found {len(unit_files):,} files")
            all_files.extend(unit_files)
        else:
            print(f"  📊 {unit_dir}: No files found")
    
    all_files.sort()
    return all_files


def filter_unwanted_files(file_paths, exclude_focus=True, exclude_test=True, debug=False):
    """Filter out unwanted files using FilenamePatternAnalyzer."""
    filtered_files = []
    exclusion_stats = {
        'focus_files': 0,
        'test_files': 0,
        'unparseable_files': 0,
        'other_exclusions': 0
    }
    
    print(f"🔽 Filtering {len(file_paths):,} files...")
    
    for file_path in file_paths:
        filename = os.path.basename(file_path)
        should_exclude = False
        exclusion_reason = None
        
        try:
            analyzer = FilenamePatternAnalyzer(filename)
            
            if not analyzer.filename_pattern:
                should_exclude = True
                exclusion_reason = 'unparseable_files'
            elif exclude_focus and (filename.upper().count('FOCUS') > 0 or 
                  'focustest' in filename.lower() or
                  'autofocus' in filename.lower()):
                should_exclude = True
                exclusion_reason = 'focus_files'
            elif exclude_test and ('TEST' in filename.upper() or 
                  'test' in filename.lower() or
                  'CALIB' in filename.upper()):
                should_exclude = True
                exclusion_reason = 'test_files'
                
        except ValueError:
            should_exclude = True
            exclusion_reason = 'unparseable_files'
        except Exception as e:
            if debug:
                print(f"  ⚠️ Error analyzing {filename}: {e}")
            should_exclude = True
            exclusion_reason = 'other_exclusions'
        
        if should_exclude:
            exclusion_stats[exclusion_reason] += 1
            if debug and exclusion_stats[exclusion_reason] <= 5:
                print(f"  🚫 Excluding ({exclusion_reason}): {filename}")
        else:
            filtered_files.append(file_path)
    
    return filtered_files, exclusion_stats


def should_create_target(object_name, frame_type):
    """Determine if an object should have a Target created."""
    if not object_name or object_name == 'UNKNOWN':
        return False
    
    if frame_type in ['BIAS', 'DARK', 'FLAT']:
        return False
    
    if frame_type != 'SCIENCE':
        return False
    
    calibration_names = ['BIAS', 'DARK', 'FLAT', 'CALIB', 'CALIBRATION']
    if object_name.upper() in calibration_names:
        return False
    
    test_patterns = ['FOCUS', 'TEST', 'AUTOFOCUS', 'FOCUSTEST']
    if any(pattern in object_name.upper() for pattern in test_patterns):
        return False
    
    if object_name.startswith('T') and len(object_name) > 1:
        tile_id_str = ''.join(filter(str.isdigit, object_name[1:]))
        if tile_id_str:
            return False
    
    return True


def get_frame_type_from_analyzer(analyzer):
    """Extract frame type from FilenamePatternAnalyzer."""
    if not analyzer.parsed_filename:
        return 'SCIENCE'  # Changed from 'UNKNOWN' to 'SCIENCE'
    
    parsed = analyzer.parsed_filename
    
    for field in ['frame_type', 'imagetyp', 'obstype']:
        if field in parsed and parsed[field]:
            frame_type = parsed[field].upper()
            # Normalize LIGHT to SCIENCE
            if frame_type == 'LIGHT':
                return 'SCIENCE'
            return frame_type
    
    if 'object_info' in parsed or 'object_name' in parsed:
        object_name = parsed.get('object_info') or parsed.get('object_name', '')
        if object_name:
            object_upper = object_name.upper()
            if 'BIAS' in object_upper:
                return 'BIAS'
            elif 'DARK' in object_upper:
                return 'DARK'
            elif 'FLAT' in object_upper:
                return 'FLAT'
    
    return 'SCIENCE'  # Changed from 'SCIENCE' to ensure science frames

def pre_process_targets(file_paths, debug=False):
    """
    Pre-process files to identify and create Target objects.
    Enhanced to rely on models.py for header parsing and filter detection.
    """
    target_stats = {
        'created': 0,
        'existing': 0,
        'tiles': 0,
        'calibration_skipped': 0,
        'targets_to_create': {}
    }
    
    print(f"🎯 Analyzing {len(file_paths):,} files for target information...")
    
    # Use sampling for faster processing on large datasets
    sample_size = min(1000, len(file_paths))
    sampled_files = file_paths[::max(1, len(file_paths) // sample_size)]
    
    for file_path in sampled_files:
        filename = os.path.basename(file_path)
        
        try:
            # Analyze filename pattern
            analyzer = FilenamePatternAnalyzer(filename)
            
            if not analyzer.parsed_filename:
                continue
            
            # Extract basic frame information
            object_name = get_object_name_from_analyzer(analyzer)
            frame_type = get_frame_type_from_analyzer(analyzer)
            
            # Skip if no valid object name found
            if not object_name or object_name == 'UNKNOWN':
                continue
            
            # Skip calibration frames - they don't need targets
            if not should_create_target(object_name, frame_type):
                if frame_type in ['BIAS', 'DARK', 'FLAT']:
                    target_stats['calibration_skipped'] += 1
                continue
            
            # Check for tile observations - they link to tiles, not targets
            if object_name.startswith('T') and len(object_name) > 1:
                try:
                    # Extract numeric part for tile ID validation
                    tile_id_str = ''.join(filter(str.isdigit, object_name[1:]))
                    if tile_id_str:
                        target_stats['tiles'] += 1
                        continue  # Skip tile observations for target creation
                except ValueError:
                    pass
            
            # Track unique targets to create
            if object_name not in target_stats['targets_to_create']:
                target_stats['targets_to_create'][object_name] = {
                    'count': 0,
                    'example_file': filename,
                    'frame_type': frame_type
                }
            
            target_stats['targets_to_create'][object_name]['count'] += 1
            
        except Exception as e:
            if debug:
                print(f"  ⚠️ Error analyzing {filename}: {e}")
            continue
    
    # Create Target objects for identified targets
    for target_name, target_info in target_stats['targets_to_create'].items():
        try:
            # Use get_or_create to avoid duplicates
            target, created = Target.objects.get_or_create(
                name=target_name,
                defaults={
                    'ra': 0.0,  # Will be updated from FITS headers later
                    'dec': 0.0,  # Will be updated from FITS headers later
                    'target_type': determine_target_type(target_name),
                    'description': f'Auto-created from observation {target_info["example_file"]}'
                }
            )
            
            if created:
                target_stats['created'] += 1
                if debug:
                    print(f"  🆕 Created target: {target_name} ({target_info['count']} files)")
            else:
                target_stats['existing'] += 1
                if debug:
                    print(f"  🔄 Found existing target: {target_name}")
                    
        except Exception as e:
            if debug:
                print(f"  ❌ Failed to create target {target_name}: {e}")
    
    return target_stats

def get_object_name_from_analyzer(analyzer):
    """Extract object name from FilenamePatternAnalyzer."""
    if not analyzer.parsed_filename:
        return None
    
    parsed = analyzer.parsed_filename
    
    for field in ['object_info', 'object_name', 'focus_or_object']:
        if field in parsed and parsed[field]:
            return parsed[field]
    
    return None


def determine_target_type(target_name):
    """Determine target type based on target name."""
    name_upper = target_name.upper()
    
    if any(pattern in name_upper for pattern in ['BIAS', 'DARK', 'FLAT', 'CALIB']):
        return 'TEST'
    
    if any(pattern in name_upper for pattern in ['GRB', 'SN', 'AT', 'TOO']):
        return 'TOO'
    
    if any(pattern in name_upper for pattern in ['LTT', 'SA', 'PG', 'FEIGE', 'WD']):
        return 'STD'
    
    if any(pattern in name_upper for pattern in ['TEST', 'FOCUS']):
        return 'TEST'
    
    return 'EXSCI'


def post_process_targets(night, debug=False):
    """Post-process science frames to link them with appropriate targets."""
    target_stats = {
        'linked_targets': 0,
        'linked_tiles': 0,
        'coordinates_updated': 0
    }
    
    if debug:
        science_frames = ScienceFrame.objects.filter(night=night)
    else:
        science_frames = ScienceFrame.objects.filter(
            night=night,
            target__isnull=True,
            tile__isnull=True
        )
    
    if debug:
        print(f"  🎯 Post-processing {science_frames.count()} science frames for target linking...")
    
    for frame in science_frames:
        try:
            object_name = frame.object_name
            
            if not object_name or object_name == 'UNKNOWN':
                continue
            
            if object_name.startswith('T') and len(object_name) > 1:
                try:
                    tile_id_str = ''.join(filter(str.isdigit, object_name[1:]))
                    if tile_id_str:
                        tile_id = int(tile_id_str)
                        tile = Tile.objects.filter(id=tile_id).first()
                        if tile:
                            if not frame.tile:
                                frame.tile = tile
                                frame.save(update_fields=['tile'])
                                target_stats['linked_tiles'] += 1
                            continue
                except ValueError:
                    pass
            
            target = Target.objects.filter(name=object_name).first()
            if target:
                if not frame.target:
                    frame.target = target
                    frame.save(update_fields=['target'])
                    target_stats['linked_targets'] += 1
                
                if (frame.object_ra and frame.object_dec and 
                    (target.ra == 0.0 or target.dec == 0.0)):
                    target.ra = frame.object_ra
                    target.dec = frame.object_dec
                    target.save(update_fields=['ra', 'dec'])
                    target_stats['coordinates_updated'] += 1
                    
        except Exception as e:
            if debug:
                print(f"    ❌ Error processing frame {frame.original_filename}: {e}")
            continue
    
    return target_stats


def main():
    """Main entry point with integrated cleanup and import."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Enhanced RAW Data Ingest with Integrated Cleanup',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Clean import (removes existing data first)
  python SIMPLE_raw_ingest.py --date 2025-06-04 --cleanup
  
  # Auto-confirm cleanup for automation
  python SIMPLE_raw_ingest.py --date 2025-06-04 --cleanup --auto-confirm
  
  # Import only (no cleanup)
  python SIMPLE_raw_ingest.py --date 2025-06-04
  
  # Debug mode with validation
  python SIMPLE_raw_ingest.py --date 2025-06-04 --cleanup --debug --validate
        """
    )
    
    parser.add_argument('--date', 
                       default='2025-06-04', 
                       help='Date to process (YYYY-MM-DD format)')
    
    parser.add_argument('--cleanup', 
                       action='store_true', 
                       help='Clean up existing data before importing')
    
    parser.add_argument('--auto-confirm', 
                       dest='auto_confirm_cleanup',
                       action='store_true', 
                       help='Auto-confirm cleanup without user interaction')
    
    parser.add_argument('--parallel', 
                       action='store_true', 
                       help='Force parallel processing')
    
    parser.add_argument('--workers', 
                       type=int, 
                       default=4, 
                       help='Number of parallel workers (default: 4)')
    
    parser.add_argument('--limit', 
                       type=int, 
                       help='Limit number of files to process')
    
    parser.add_argument('--debug', 
                       action='store_true', 
                       help='Enable debug mode')
    
    parser.add_argument('--validate', 
                       action='store_true', 
                       help='Run validation checks after import')
    
    parser.add_argument('--no-create-targets', 
                       dest='create_targets',
                       action='store_false', 
                       help='Disable automatic Target creation')
    
    parser.add_argument('--no-exclude-focus', 
                       dest='exclude_focus',
                       action='store_false', 
                       help='Include focus-related files')
    
    parser.add_argument('--no-exclude-test', 
                       dest='exclude_test',
                       action='store_false', 
                       help='Include test/calibration files')
    
    parser.set_defaults(create_targets=True, exclude_focus=True, exclude_test=True)
    
    args = parser.parse_args()
    
    try:
        date.fromisoformat(args.date)
    except ValueError:
        print(f"❌ Invalid date format: {args.date}")
        print("   Expected format: YYYY-MM-DD (e.g., 2025-06-04)")
        sys.exit(1)
    
    try:
        enhanced_ingest(
            date_str=args.date,
            cleanup=args.cleanup,
            auto_confirm_cleanup=args.auto_confirm_cleanup,
            parallel=args.parallel,
            max_workers=args.workers,
            limit=args.limit,
            debug=args.debug,
            validate=args.validate,
            create_targets=args.create_targets,
            exclude_focus=args.exclude_focus,
            exclude_test=args.exclude_test
        )
    except KeyboardInterrupt:
        print("\n🛑 Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Process failed with error: {e}")
        if args.debug:
            import traceback
            traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
