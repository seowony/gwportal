#!/usr/bin/env python3
"""
Enhanced missing files analysis script with flexible date filtering options
Supports full analysis or specific date range processing
"""

import os
import sys
import django
import glob
from collections import defaultdict
import re
from datetime import datetime
import argparse

# Django 설정
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'gwportal.settings')
django.setup()

from survey.models import ScienceFrame, BiasFrame, DarkFrame, FlatFrame

def is_science_file(filename):
    """Check if the file is actual science observation data"""
    filename_lower = filename.lower()
    
    # Patterns to exclude
    exclude_patterns = [
        'focus', 'test', 'master', 'calib', 'lamp', 'twilight', 
        'snapshot', 'corsub', 'sub_', 'autofocus', 'af_',
        'defocus_test',  # Defocus_test is test data, so exclude
    ]
    
    # Special filenames (exact match)
    special_excludes = [
        'bias.fits', 'mask60.fits', 'snapshot'
    ]
    
    # Check exact match
    if filename_lower in special_excludes:
        return False
    
    # Check pattern inclusion
    for pattern in exclude_patterns:
        if pattern in filename_lower:
            return False
    
    return True

def get_filename_pattern(filename):
    """Extract unique pattern from filename"""
    parts = filename.split('_')
    if len(parts) >= 6:
        pattern_parts = parts.copy()
        pattern_parts[3] = '[OBJECT]'  # Replace object name with placeholder
        
        # Remove number from last part (0001.fits -> [NUM].fits)
        last_part = pattern_parts[-1]
        if last_part.endswith('.fits'):
            number_part = last_part.replace('.fits', '')
            if number_part.isdigit():
                pattern_parts[-1] = '[NUM].fits'
        
        return '_'.join(pattern_parts)
    return filename

def get_files_by_date_range(start_date=None, end_date=None, specific_dates=None):
    """
    Get FITS files filtered by date range or specific dates
    Handles both filename dates and folder dates for proper filtering
    
    Args:
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format  
        specific_dates (list): List of specific dates in YYYY-MM-DD format
    
    Returns:
        tuple: (all_fits_files, filtered_fits_files, file_date_info)
    """
    # Get all FITS files
    all_fits_files = [f for f in glob.glob('/lyman/data1/obsdata/7DT*/*/*.fits')]
    
    # Extract date information for each file
    file_date_info = {}
    
    for file_path in all_fits_files:
        filename = os.path.basename(file_path)
        folder_path = os.path.dirname(file_path)
        folder_name = os.path.basename(folder_path)
        
        # Extract date from filename (7DT*_YYYYMMDD_* pattern)
        filename_match = re.search(r'7DT\d+_(\d{8})_', filename)
        filename_date = None
        if filename_match:
            date_str = filename_match.group(1)
            filename_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        
        # Extract date from folder name (YYYY-MM-DD_* pattern)
        folder_match = re.search(r'(\d{4}-\d{2}-\d{2})', folder_name)
        folder_date = folder_match.group(1) if folder_match else None
        
        file_date_info[file_path] = {
            'filename': filename,
            'filename_date': filename_date,
            'folder_date': folder_date,
            'folder_name': folder_name
        }
    
    if not start_date and not end_date and not specific_dates:
        # Return all files if no filtering specified
        return all_fits_files, all_fits_files, file_date_info
    
    filtered_files = []
    
    for file_path in all_fits_files:
        info = file_date_info[file_path]
        filename_date = info['filename_date']
        folder_date = info['folder_date']
        
        # Use filename date for filtering (primary), fallback to folder date
        filter_date = filename_date if filename_date else folder_date
        
        if not filter_date:
            continue
        
        # Filter by specific dates
        if specific_dates:
            if filter_date in specific_dates:
                filtered_files.append(file_path)
        # Filter by date range
        elif start_date or end_date:
            include_file = True
            if start_date and filter_date < start_date:
                include_file = False
            if end_date and filter_date > end_date:
                include_file = False
            if include_file:
                filtered_files.append(file_path)
    
    return all_fits_files, filtered_files, file_date_info

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Analyze missing FITS files with flexible date filtering',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Quick list of folders with missing files only
  python %(prog)s --list-missing-folders
  
  # List missing folders for specific dates
  python %(prog)s --list-missing-folders --dates 2025-02-27 2025-06-29
  
  # Analyze by folders (RECOMMENDED - most accurate)
  python %(prog)s --by-folders
  
  # Analyze specific folder dates
  python %(prog)s --by-folders --dates 2025-02-19 2025-02-28 2025-06-29
  
  # Analyze folder date range  
  python %(prog)s --by-folders --start-date 2025-02-01 --end-date 2025-02-28
  
  # Quick folder summary only
  python %(prog)s --by-folders --summary-only
  
  # Traditional filename-based analysis
  python %(prog)s --dates 2025-02-19 2025-02-28
        """)
    
    parser.add_argument('--dates', nargs='+', metavar='YYYY-MM-DD',
                       help='Specific dates to analyze (e.g., 2025-02-19 2025-02-28)')
    
    parser.add_argument('--start-date', metavar='YYYY-MM-DD',
                       help='Start date for range analysis (YYYY-MM-DD format)')
    
    parser.add_argument('--end-date', metavar='YYYY-MM-DD', 
                       help='End date for range analysis (YYYY-MM-DD format)')
    
    parser.add_argument('--summary-only', action='store_true',
                       help='Show only summary without creating detailed log file')
    
    parser.add_argument('--output', metavar='FILENAME',
                       help='Custom output log filename')
    
    parser.add_argument('--show-example-dates', action='store_true',
                       help='Show example dates from previous analysis runs')
    
    parser.add_argument('--analyze-date-mismatches', action='store_true',
                       help='Focus on analyzing files with filename/folder date mismatches')
    
    parser.add_argument('--by-folders', action='store_true',
                       help='Analyze missing files by folder structure (recommended)')
    
    parser.add_argument('--list-missing-folders', action='store_true',
                       help='List only folders with missing files (quick overview)')
    
    return parser.parse_args()

def analyze_and_save_missing_files(start_date=None, end_date=None, specific_dates=None, 
                                 summary_only=False, output_filename=None):
    """
    Analyze missing files and save log with flexible filtering options
    
    Args:
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
        specific_dates (list): List of specific dates to analyze
        summary_only (bool): If True, only show summary without detailed log
        output_filename (str): Custom output filename
    """
    
    # Generate timestamp and filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    if output_filename:
        log_filename = output_filename
    else:
        # Create descriptive filename based on filtering options
        if specific_dates:
            date_suffix = f"dates_{'_'.join(specific_dates).replace('-', '')}"
        elif start_date or end_date:
            start_str = start_date.replace('-', '') if start_date else 'start'
            end_str = end_date.replace('-', '') if end_date else 'end'
            date_suffix = f"range_{start_str}_to_{end_str}"
        else:
            date_suffix = "all"
        
        log_filename = f'missing_files_analysis_{date_suffix}_{timestamp}.log'
    
    # Filter description for display
    if specific_dates:
        filter_desc = f"Specific dates: {', '.join(specific_dates)}"
    elif start_date or end_date:
        start_str = start_date if start_date else "beginning"
        end_str = end_date if end_date else "end"
        filter_desc = f"Date range: {start_str} to {end_str}"
    else:
        filter_desc = "All dates"
    
    print("📅 Enhanced Missing Files Analysis")
    print("=" * 80)
    print(f"🔍 Filter: {filter_desc}")
    print("=" * 80)
    
    # 1. Data collection with filtering
    print("📊 Collecting data...")
    
    # Get database files
    db_files = set(ScienceFrame.objects.values_list('original_filename', flat=True)) | \
               set(BiasFrame.objects.values_list('original_filename', flat=True)) | \
               set(DarkFrame.objects.values_list('original_filename', flat=True)) | \
               set(FlatFrame.objects.values_list('original_filename', flat=True))
    
    # Get filtered FITS files
    all_fits_files, filtered_fits_files, file_date_info = get_files_by_date_range(start_date, end_date, specific_dates)
    all_filenames = [os.path.basename(f) for f in all_fits_files]
    filtered_filenames = [os.path.basename(f) for f in filtered_fits_files]
    
    # Analyze date mismatches
    date_mismatches = []
    for file_path in filtered_fits_files:
        info = file_date_info[file_path]
        if info['filename_date'] and info['folder_date']:
            if info['filename_date'] != info['folder_date']:
                date_mismatches.append({
                    'filename': info['filename'],
                    'filename_date': info['filename_date'],
                    'folder_date': info['folder_date'],
                    'folder_name': info['folder_name'],
                    'full_path': file_path
                })
    
    # Filter science observation files only
    science_files = set()
    excluded_files = set()
    
    for filename in filtered_filenames:
        if is_science_file(filename):
            science_files.add(filename)
        else:
            excluded_files.add(filename)
    
    fits_files = science_files
    missing_files = fits_files - db_files
    
    print(f"   Total FITS files (all): {len(all_filenames):,}")
    print(f"   Filtered FITS files: {len(filtered_filenames):,}")
    print(f"   Excluded by filtering: {len(excluded_files):,}")
    print(f"   Science observation files: {len(fits_files):,}")
    print(f"   DB registered files: {len(db_files):,}")
    print(f"   Missing files: {len(missing_files):,}")
    
    # Show date mismatch information
    if date_mismatches:
        print(f"   ⚠️  Date mismatches found: {len(date_mismatches):,} files")
        print(f"      (files where filename date ≠ folder date)")
    
    # Show sample mismatches
    if date_mismatches and len(date_mismatches) <= 10:
        print(f"\n📋 Date Mismatch Examples:")
        for i, mismatch in enumerate(date_mismatches[:10], 1):
            print(f"   {i}. {mismatch['filename']}")
            print(f"      Filename date: {mismatch['filename_date']}")
            print(f"      Folder date: {mismatch['folder_date']} ({mismatch['folder_name']})")
    elif date_mismatches and len(date_mismatches) > 10:
        print(f"\n📋 Sample Date Mismatches (showing first 5):")
        for i, mismatch in enumerate(date_mismatches[:5], 1):
            print(f"   {i}. {mismatch['filename']}")
            print(f"      Filename: {mismatch['filename_date']} vs Folder: {mismatch['folder_date']}")
    
    
    # 2. Group by date
    print("\n📝 Classifying by date...")
    date_missing = defaultdict(list)
    
    for filename in missing_files:
        # Extract date (7DT*_YYYYMMDD_* pattern)  
        match = re.search(r'7DT\d+_(\d{8})_', filename)
        if match:
            date_str = match.group(1)
            formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            date_missing[formatted_date].append(filename)
        else:
            # Special files without date pattern
            date_missing['SPECIAL'].append(filename)
    
    # Console summary
    valid_dates = [d for d in date_missing.keys() if d != 'SPECIAL']
    total_missing = sum(len(files) for files in date_missing.values())
    
    print(f"\n📅 Date-wise Missing Files Summary:")
    print("-" * 40)
    
    for date in sorted(date_missing.keys()):
        files = date_missing[date]
        if files:
            print(f"{date}: {len(files):,}")
    
    print("-" * 40)
    print(f"📊 Total missing dates: {len(valid_dates)}")
    print(f"📊 Total missing files: {total_missing:,}")
    
    # Top missing dates
    sorted_dates = sorted([(date, len(files)) for date, files in date_missing.items() if date != 'SPECIAL'], 
                         key=lambda x: x[1], reverse=True)
    
    if sorted_dates:
        print(f"\n📈 Top Missing Dates (TOP 10):")
        for i, (date, count) in enumerate(sorted_dates[:10], 1):
            print(f"  {i:2d}. {date}: {count:,}")
    
    if 'SPECIAL' in date_missing and date_missing['SPECIAL']:
        print(f"\n🔍 Special files: {len(date_missing['SPECIAL'])}")
    
    # Skip detailed log if summary-only mode
    if summary_only:
        print(f"\n✅ Summary completed (detailed log skipped)")
        return
    
    # 3. Create detailed log file
    print(f"\n💾 Saving detailed log... ({log_filename})")
    
    with open(log_filename, 'w', encoding='utf-8') as log_file:
        # Log header
        log_file.write("=" * 100 + "\n")
        log_file.write(f"Missing Files Analysis Detailed Log\n")
        log_file.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        log_file.write(f"Filter: {filter_desc}\n")
        log_file.write("=" * 100 + "\n\n")
        
        # Overall summary
        log_file.write("📊 Overall Summary:\n")
        log_file.write(f"   • Total FITS files (all): {len(all_filenames):,}\n")
        log_file.write(f"   • Filtered FITS files: {len(filtered_filenames):,}\n")
        log_file.write(f"   • Excluded by filtering: {len(excluded_files):,}\n")
        log_file.write(f"   • Science observation files: {len(fits_files):,}\n")
        log_file.write(f"   • DB registered files: {len(db_files):,}\n")
        log_file.write(f"   • Missing files: {len(missing_files):,}\n")
        log_file.write(f"   • Missing dates count: {len(valid_dates)}\n")
        
        if date_mismatches:
            log_file.write(f"   • Date mismatches found: {len(date_mismatches):,}\n")
        log_file.write("\n")
        
        # Date mismatch section
        if date_mismatches:
            log_file.write("⚠️  Date Mismatch Analysis:\n")
            log_file.write("=" * 50 + "\n")
            log_file.write("Files where filename date differs from folder date:\n\n")
            
            # Group mismatches by type
            mismatch_groups = defaultdict(list)
            for mismatch in date_mismatches:
                key = f"{mismatch['folder_date']} → {mismatch['filename_date']}"
                mismatch_groups[key].append(mismatch)
            
            for i, (pattern, files) in enumerate(sorted(mismatch_groups.items()), 1):
                log_file.write(f"{i}. {pattern} ({len(files)} files)\n")
                for j, file_info in enumerate(files[:5], 1):  # Show first 5 examples
                    log_file.write(f"   {j}. {file_info['filename']} (in {file_info['folder_name']})\n")
                if len(files) > 5:
                    log_file.write(f"   ... and {len(files) - 5} more files\n")
                log_file.write("\n")
            
            log_file.write("=" * 80 + "\n\n")
        
        # Date-wise detailed information
        log_file.write("📅 Date-wise Detailed Analysis:\n")
        log_file.write("=" * 80 + "\n\n")
        
        for date in sorted(date_missing.keys()):
            files = date_missing[date]
            
            if not files:
                continue
            
            # Detailed info in log
            log_file.write(f"📅 {date}: {len(files):,} files\n")
            log_file.write("-" * 50 + "\n")
            
            # Pattern classification
            patterns = defaultdict(list)
            for filename in files:
                pattern = get_filename_pattern(filename)
                patterns[pattern].append(filename)
            
            # Pattern summary
            log_file.write("📋 Pattern Summary:\n")
            for i, (pattern, pattern_files) in enumerate(sorted(patterns.items()), 1):
                log_file.write(f"   {i}. {pattern} ({len(pattern_files)} files)\n")
            
            # Complete file list
            log_file.write(f"\n📁 Complete File List ({len(files)} files):\n")
            for i, filename in enumerate(sorted(files), 1):
                log_file.write(f"   {i:4d}. {filename}\n")
            
            log_file.write("\n" + "=" * 80 + "\n\n")
        
        # Special files
        if 'SPECIAL' in date_missing and date_missing['SPECIAL']:
            special_files = date_missing['SPECIAL']
            log_file.write(f"🔍 Special Files ({len(special_files)} files):\n")
            log_file.write("-" * 30 + "\n")
            for i, filename in enumerate(sorted(special_files), 1):
                log_file.write(f"   {i:2d}. {filename}\n")
            log_file.write("\n")
        
        # Final summary
        log_file.write("=" * 100 + "\n")
        log_file.write("📈 Final Summary:\n")
        log_file.write(f"   • Analyzed dates: {len(valid_dates)}\n")
        log_file.write(f"   • Total missing files: {total_missing:,}\n")
        
        # Top missing dates in log
        if sorted_dates:
            log_file.write(f"   • Top Missing Dates (TOP 10):\n")
            for i, (date, count) in enumerate(sorted_dates[:10], 1):
                log_file.write(f"     {i:2d}. {date}: {count:,}\n")
    
    print(f"\n✅ Detailed log saved to '{log_filename}'")
    print(f"📝 Log file size: {os.path.getsize(log_filename) / 1024 / 1024:.2f} MB")

def extract_dates_from_previous_run():
    """
    Utility function to help extract problematic dates from previous run output
    This can be used as a reference for --dates parameter
    """
    # Example dates from the user's previous run
    example_dates = [
        '2025-02-14', '2025-02-18', '2025-02-19', '2025-02-20', '2025-02-21',
        '2025-02-22', '2025-02-23', '2025-02-24', '2025-02-25', '2025-02-26',
        '2025-02-27', '2025-02-28', '2025-03-01', '2025-03-02', '2025-03-03',
        '2025-03-04', '2025-03-05', '2025-06-28', '2025-06-29'
    ]
    
    # Top problematic dates (>1000 missing files)
    top_problematic = ['2025-06-29', '2025-02-19', '2025-02-28', '2025-02-20']
    
    print("📋 Reference: Dates from previous analysis")
    print("=" * 50)
    print("🔍 All problematic dates:")
    print("   " + " ".join(example_dates))
    print(f"\n🚨 Top problematic dates (>1000 missing files):")
    print("   " + " ".join(top_problematic))
    print(f"\n💡 Usage examples:")
    print(f"   # Analyze top problematic dates only:")
    print(f"   python {sys.argv[0]} --dates {' '.join(top_problematic)}")
    print(f"   # Analyze February 2025 range:")
    print(f"   python {sys.argv[0]} --start-date 2025-02-01 --end-date 2025-02-28")

def analyze_date_mismatches():
    """
    Analyze files where filename dates don't match folder dates
    This helps identify files that might be in wrong folders or have incorrect dates
    """
    print("🔍 Date Mismatch Analysis")
    print("=" * 80)
    print("Analyzing files where filename date ≠ folder date...")
    print("=" * 80)
    
    # Get all files with date information
    all_fits_files, _, file_date_info = get_files_by_date_range()
    
    # Find mismatches
    mismatches = []
    for file_path, info in file_date_info.items():
        if info['filename_date'] and info['folder_date']:
            if info['filename_date'] != info['folder_date']:
                mismatches.append(info)
    
    if not mismatches:
        print("✅ No date mismatches found!")
        return
    
    print(f"⚠️  Found {len(mismatches):,} files with date mismatches")
    print()
    
    # Group by mismatch pattern
    patterns = defaultdict(list)
    for mismatch in mismatches:
        pattern = f"{mismatch['folder_date']} → {mismatch['filename_date']}"
        patterns[pattern].append(mismatch)
    
    # Display patterns
    print("📊 Mismatch Patterns:")
    print("-" * 50)
    
    for i, (pattern, files) in enumerate(sorted(patterns.items(), key=lambda x: len(x[1]), reverse=True), 1):
        print(f"{i:2d}. {pattern}: {len(files):,} files")
        
        # Show a few examples
        for j, file_info in enumerate(files[:3], 1):
            print(f"     {j}. {file_info['filename']}")
            print(f"        Folder: {file_info['folder_name']}")
        
        if len(files) > 3:
            print(f"     ... and {len(files) - 3:,} more files")
        print()
    
    # Create detailed report
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_filename = f'date_mismatch_analysis_{timestamp}.log'
    
    print(f"💾 Saving detailed report to: {report_filename}")
    
    with open(report_filename, 'w', encoding='utf-8') as f:
        f.write("=" * 100 + "\n")
        f.write("Date Mismatch Analysis Report\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 100 + "\n\n")
        
        f.write(f"📊 Summary:\n")
        f.write(f"   • Total files analyzed: {len(file_date_info):,}\n")
        f.write(f"   • Files with date mismatches: {len(mismatches):,}\n")
        f.write(f"   • Unique mismatch patterns: {len(patterns)}\n\n")
        
        f.write("🔍 Detailed Mismatch Analysis:\n")
        f.write("=" * 80 + "\n\n")
        
        for i, (pattern, files) in enumerate(sorted(patterns.items(), key=lambda x: len(x[1]), reverse=True), 1):
            f.write(f"{i}. Pattern: {pattern} ({len(files):,} files)\n")
            f.write("-" * 60 + "\n")
            
            for j, file_info in enumerate(files, 1):
                f.write(f"   {j:4d}. {file_info['filename']}\n")
                f.write(f"          Folder: {file_info['folder_name']}\n")
                f.write(f"          Filename Date: {file_info['filename_date']}\n")
                f.write(f"          Folder Date: {file_info['folder_date']}\n")
            
            f.write("\n" + "=" * 80 + "\n\n")
    
    print(f"✅ Report saved: {report_filename}")
    print(f"📝 File size: {os.path.getsize(report_filename) / 1024:.1f} KB")

def analyze_by_folders(start_date=None, end_date=None, specific_dates=None, 
                    summary_only=False, output_filename=None):
    """
    Analyze missing files by folder structure instead of filename dates
    This is more accurate for checking DB registration status by observation session
    
    Args:
        start_date (str): Start date in YYYY-MM-DD format (for folder filtering)
        end_date (str): End date in YYYY-MM-DD format (for folder filtering)
        specific_dates (list): List of specific folder dates to analyze
        summary_only (bool): If True, only show summary without detailed log
        output_filename (str): Custom output filename
    """
    
    # Generate timestamp and filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    if output_filename:
        log_filename = output_filename
    else:
        # Create descriptive filename based on filtering options
        if specific_dates:
            date_suffix = f"folders_{'_'.join(specific_dates).replace('-', '')}"
        elif start_date or end_date:
            start_str = start_date.replace('-', '') if start_date else 'start'
            end_str = end_date.replace('-', '') if end_date else 'end'
            date_suffix = f"folder_range_{start_str}_to_{end_str}"
        else:
            date_suffix = "all_folders"
        
        log_filename = f'missing_files_analysis_{date_suffix}_{timestamp}.log'
    
    # Filter description for display
    if specific_dates:
        filter_desc = f"Specific folder dates: {', '.join(specific_dates)}"
    elif start_date or end_date:
        start_str = start_date if start_date else "beginning"
        end_str = end_date if end_date else "end"
        filter_desc = f"Folder date range: {start_str} to {end_str}"
    else:
        filter_desc = "All folders"
    
    print("📁 Folder-based Missing Files Analysis")
    print("=" * 80) 
    print(f"🔍 Filter: {filter_desc}")
    print("=" * 80)
    
    # 1. Get database files
    print("📊 Collecting database files...")
    db_files = set(ScienceFrame.objects.values_list('original_filename', flat=True)) | \
               set(BiasFrame.objects.values_list('original_filename', flat=True)) | \
               set(DarkFrame.objects.values_list('original_filename', flat=True)) | \
               set(FlatFrame.objects.values_list('original_filename', flat=True))
    
    print(f"   DB registered files: {len(db_files):,}")
    
    # 2. Analyze folders
    print("📊 Analyzing folder structure...")
    
    # Get all observation folders
    base_path = '/lyman/data1/obsdata'
    telescope_dirs = glob.glob(f'{base_path}/7DT*')
    
    folder_analysis = {}
    total_files = 0
    total_science_files = 0
    total_missing = 0
    
    for telescope_dir in sorted(telescope_dirs):
        telescope_name = os.path.basename(telescope_dir)
        date_folders = glob.glob(f'{telescope_dir}/*')
        
        for folder_path in sorted(date_folders):
            if not os.path.isdir(folder_path):
                continue
                
            folder_name = os.path.basename(folder_path)
            
            # Extract folder date
            folder_match = re.search(r'(\d{4}-\d{2}-\d{2})', folder_name)
            if not folder_match:
                continue
                
            folder_date = folder_match.group(1)
            
            # Apply date filtering
            if specific_dates and folder_date not in specific_dates:
                continue
            if start_date and folder_date < start_date:
                continue
            if end_date and folder_date > end_date:
                continue
            
            # Get all FITS files in this folder
            fits_files = glob.glob(f'{folder_path}/*.fits')
            all_filenames = [os.path.basename(f) for f in fits_files]
            
            # Filter science files
            science_filenames = [f for f in all_filenames if is_science_file(f)]
            excluded_filenames = [f for f in all_filenames if not is_science_file(f)]
            
            # Check which science files are missing from DB
            missing_filenames = [f for f in science_filenames if f not in db_files]
            registered_filenames = [f for f in science_filenames if f in db_files]
            
            # Store analysis results
            folder_key = f"{telescope_name}/{folder_name}"
            folder_analysis[folder_key] = {
                'telescope': telescope_name,
                'folder_name': folder_name,
                'folder_date': folder_date,
                'folder_path': folder_path,
                'total_files': len(all_filenames),
                'science_files': len(science_filenames),
                'excluded_files': len(excluded_filenames),
                'registered_files': len(registered_filenames),
                'missing_files': len(missing_filenames),
                'missing_list': missing_filenames,
                'registered_list': registered_filenames,
                'excluded_list': excluded_filenames
            }
            
            total_files += len(all_filenames)
            total_science_files += len(science_filenames)
            total_missing += len(missing_filenames)
    
    print(f"   Total folders analyzed: {len(folder_analysis)}")
    print(f"   Total FITS files: {total_files:,}")
    print(f"   Total science files: {total_science_files:,}")
    print(f"   Total missing files: {total_missing:,}")
    
    # 3. Display results
    print(f"\n📁 Folder-wise Analysis Results:")
    print("=" * 80)
    
    # Sort folders by missing file count (descending)
    sorted_folders = sorted(folder_analysis.items(), 
                           key=lambda x: x[1]['missing_files'], 
                           reverse=True)
    
    folders_with_missing = [item for item in sorted_folders if item[1]['missing_files'] > 0]
    folders_complete = [item for item in sorted_folders if item[1]['missing_files'] == 0]
    
    print(f"📊 Summary:")
    print(f"   • Folders with missing files: {len(folders_with_missing)}")
    print(f"   • Complete folders: {len(folders_complete)}")
    print(f"   • Total missing files: {total_missing:,}")
    
    if folders_with_missing:
        print(f"\n🚨 Folders with Missing Files (Top 20):")
        print("-" * 80)
        print(f"{'Folder':<40} {'Date':<12} {'Total':<8} {'Science':<8} {'Missing':<8} {'%Missing':<8}")
        print("-" * 80)
        
        for folder_key, data in folders_with_missing[:20]:
            missing_pct = (data['missing_files'] / data['science_files'] * 100) if data['science_files'] > 0 else 0
            print(f"{folder_key:<40} {data['folder_date']:<12} {data['total_files']:<8} "
                  f"{data['science_files']:<8} {data['missing_files']:<8} {missing_pct:>6.1f}%")
    
    # Skip detailed log if summary-only mode
    if summary_only:
        print(f"\n✅ Summary completed (detailed log skipped)")
        return
    
    # 4. Create detailed log
    print(f"\n💾 Saving detailed log... ({log_filename})")
    
    with open(log_filename, 'w', encoding='utf-8') as log_file:
        # Log header
        log_file.write("=" * 100 + "\n")
        log_file.write(f"Folder-based Missing Files Analysis Log\n")
        log_file.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        log_file.write(f"Filter: {filter_desc}\n")
        log_file.write("=" * 100 + "\n\n")
        
        # Overall summary
        log_file.write("📊 Overall Summary:\n")
        log_file.write(f"   • Total folders analyzed: {len(folder_analysis)}\n")
        log_file.write(f"   • Total FITS files: {total_files:,}\n")
        log_file.write(f"   • Total science files: {total_science_files:,}\n")
        log_file.write(f"   • Total missing files: {total_missing:,}\n")
        log_file.write(f"   • Folders with missing files: {len(folders_with_missing)}\n")
        log_file.write(f"   • Complete folders: {len(folders_complete)}\n\n")
        
        # Detailed folder analysis
        log_file.write("📁 Detailed Folder Analysis:\n")
        log_file.write("=" * 80 + "\n\n")
        
        for folder_key, data in sorted_folders:
            if data['missing_files'] == 0:
                continue  # Skip complete folders in detailed log
                
            log_file.write(f"📁 {folder_key}\n")
            log_file.write(f"   Date: {data['folder_date']}\n")
            log_file.write(f"   Path: {data['folder_path']}\n")
            log_file.write(f"   Total files: {data['total_files']}\n")
            log_file.write(f"   Science files: {data['science_files']}\n")
            log_file.write(f"   Registered files: {data['registered_files']}\n")
            log_file.write(f"   Missing files: {data['missing_files']}\n")
            log_file.write(f"   Excluded files: {data['excluded_files']}\n")
            
            missing_pct = (data['missing_files'] / data['science_files'] * 100) if data['science_files'] > 0 else 0
            log_file.write(f"   Missing percentage: {missing_pct:.1f}%\n")
            log_file.write("\n")
            
            # Missing files list
            if data['missing_files'] > 0:
                log_file.write("   🚨 Missing Files:\n")
                for i, filename in enumerate(sorted(data['missing_list']), 1):
                    log_file.write(f"      {i:3d}. {filename}\n")
                log_file.write("\n")
            
            # Pattern analysis for missing files
            if data['missing_files'] > 0:
                patterns = defaultdict(list)
                for filename in data['missing_list']:
                    pattern = get_filename_pattern(filename)
                    patterns[pattern].append(filename)
                
                log_file.write("   📋 Missing File Patterns:\n")
                for i, (pattern, files) in enumerate(sorted(patterns.items()), 1):
                    log_file.write(f"      {i}. {pattern} ({len(files)} files)\n")
                log_file.write("\n")
            
            log_file.write("-" * 80 + "\n\n")
        
        # Complete folders summary
        if folders_complete:
            log_file.write("✅ Complete Folders (All files registered):\n")
            log_file.write("-" * 50 + "\n")
            for folder_key, data in folders_complete:
                log_file.write(f"   {folder_key} ({data['folder_date']}) - {data['science_files']} files\n")
            log_file.write("\n")
        
        # Final summary
        log_file.write("=" * 100 + "\n")
        log_file.write("📈 Final Summary:\n")
        log_file.write(f"   • Analyzed folders: {len(folder_analysis)}\n")
        log_file.write(f"   • Total missing files: {total_missing:,}\n")
        log_file.write(f"   • Folders needing attention: {len(folders_with_missing)}\n")
        
        if folders_with_missing:
            log_file.write(f"   • Top problematic folders:\n")
            for i, (folder_key, data) in enumerate(folders_with_missing[:10], 1):
                missing_pct = (data['missing_files'] / data['science_files'] * 100) if data['science_files'] > 0 else 0
                log_file.write(f"     {i:2d}. {folder_key}: {data['missing_files']} files ({missing_pct:.1f}%)\n")
    
    print(f"\n✅ Detailed log saved to '{log_filename}'")
    print(f"📝 Log file size: {os.path.getsize(log_filename) / 1024 / 1024:.2f} MB")

def list_missing_folders_only(start_date=None, end_date=None, specific_dates=None):
    """
    Quick list of folders with missing files only - no detailed logs
    Perfect for getting a quick overview of problematic folders
    """
    
    # Filter description for display
    if specific_dates:
        filter_desc = f"Specific folder dates: {', '.join(specific_dates)}"
    elif start_date or end_date:
        start_str = start_date if start_date else "beginning"
        end_str = end_date if end_date else "end"
        filter_desc = f"Folder date range: {start_str} to {end_str}"
    else:
        filter_desc = "All folders"
    
    print("📋 Missing Folders Quick List")
    print("=" * 80)
    print(f"🔍 Filter: {filter_desc}")
    print("=" * 80)
    
    # Get database files
    print("📊 Collecting database files...")
    db_files = set(ScienceFrame.objects.values_list('original_filename', flat=True)) | \
               set(BiasFrame.objects.values_list('original_filename', flat=True)) | \
               set(DarkFrame.objects.values_list('original_filename', flat=True)) | \
               set(FlatFrame.objects.values_list('original_filename', flat=True))
    
    print(f"   DB registered files: {len(db_files):,}")
    
    # Analyze folders quickly
    print("📊 Scanning folders...")
    
    base_path = '/lyman/data1/obsdata'
    telescope_dirs = glob.glob(f'{base_path}/7DT*')
    
    missing_folders = []
    total_folders = 0
    
    for telescope_dir in sorted(telescope_dirs):
        telescope_name = os.path.basename(telescope_dir)
        date_folders = glob.glob(f'{telescope_dir}/*')
        
        for folder_path in sorted(date_folders):
            if not os.path.isdir(folder_path):
                continue
                
            folder_name = os.path.basename(folder_path)
            
            # Extract folder date
            folder_match = re.search(r'(\d{4}-\d{2}-\d{2})', folder_name)
            if not folder_match:
                continue
                
            folder_date = folder_match.group(1)
            
            # Apply date filtering
            if specific_dates and folder_date not in specific_dates:
                continue
            if start_date and folder_date < start_date:
                continue
            if end_date and folder_date > end_date:
                continue
            
            total_folders += 1
            
            # Get science files in this folder
            fits_files = glob.glob(f'{folder_path}/*.fits')
            all_filenames = [os.path.basename(f) for f in fits_files]
            science_filenames = [f for f in all_filenames if is_science_file(f)]
            
            # Check for missing files
            missing_count = len([f for f in science_filenames if f not in db_files])
            
            if missing_count > 0:
                missing_folders.append({
                    'folder_key': f"{telescope_name}/{folder_name}",
                    'telescope': telescope_name,
                    'folder_name': folder_name,
                    'folder_date': folder_date,
                    'folder_path': folder_path,
                    'total_files': len(all_filenames),
                    'science_files': len(science_filenames),
                    'missing_files': missing_count,
                    'missing_pct': (missing_count / len(science_filenames) * 100) if science_filenames else 0
                })
    
    # Sort by missing file count (descending)
    missing_folders.sort(key=lambda x: x['missing_files'], reverse=True)
    
    print(f"   Total folders scanned: {total_folders}")
    print(f"   Folders with missing files: {len(missing_folders)}")
    
    if not missing_folders:
        print("✅ No folders with missing files found!")
        return
    
    print(f"\n📋 Folders with Missing Files:")
    print("=" * 100)
    print(f"{'#':<3} {'Folder':<45} {'Date':<12} {'Total':<7} {'Science':<8} {'Missing':<8} {'%Missing':<8}")
    print("=" * 100)
    
    for i, folder in enumerate(missing_folders, 1):
        print(f"{i:<3} {folder['folder_key']:<45} {folder['folder_date']:<12} "
              f"{folder['total_files']:<7} {folder['science_files']:<8} "
              f"{folder['missing_files']:<8} {folder['missing_pct']:>6.1f}%")
    
    # Summary statistics
    total_missing = sum(f['missing_files'] for f in missing_folders)
    total_science = sum(f['science_files'] for f in missing_folders)
    
    print("=" * 100)
    print(f"📊 Summary:")
    print(f"   • Total folders with issues: {len(missing_folders)}")
    print(f"   • Total missing files: {total_missing:,}")
    print(f"   • Total science files in problematic folders: {total_science:,}")
    print(f"   • Overall missing rate: {(total_missing/total_science*100):.1f}%")
    
    # Show 100% missing folders (completely unprocessed)
    complete_missing = [f for f in missing_folders if f['missing_pct'] == 100.0]
    if complete_missing:
        print(f"\n🚨 Completely Unprocessed Folders ({len(complete_missing)} folders):")
        print("   (These folders have 0% DB registration - highest priority)")
        for folder in complete_missing:
            print(f"   • {folder['folder_key']} ({folder['folder_date']}) - {folder['missing_files']} files")
    
    # Show partially missing folders
    partial_missing = [f for f in missing_folders if 0 < f['missing_pct'] < 100.0]
    if partial_missing:
        print(f"\n⚠️  Partially Processed Folders ({len(partial_missing)} folders):")
        print("   (These folders have some files registered but some missing)")
        for folder in partial_missing[:10]:  # Show top 10
            print(f"   • {folder['folder_key']} ({folder['folder_date']}) - "
                  f"{folder['missing_files']}/{folder['science_files']} missing ({folder['missing_pct']:.1f}%)")
        if len(partial_missing) > 10:
            print(f"   ... and {len(partial_missing) - 10} more partially processed folders")
    
    print(f"\n💡 Tip: Use --by-folders with --dates to analyze specific folders in detail")
    print(f"   Example: python {sys.argv[0]} --by-folders --dates {' '.join([f['folder_date'] for f in missing_folders[:3]])}")

if __name__ == "__main__":
    try:
        # Parse command line arguments
        args = parse_arguments()
        
        # Show example dates if requested
        if args.show_example_dates:
            extract_dates_from_previous_run()
            sys.exit(0)
        
        # Analyze date mismatches if requested
        if args.analyze_date_mismatches:
            analyze_date_mismatches()
            sys.exit(0)
        
        # Use folder-based analysis if requested
        if args.by_folders:
            analyze_by_folders(
                start_date=args.start_date,
                end_date=args.end_date,
                specific_dates=args.dates,
                summary_only=args.summary_only,
                output_filename=args.output
            )
            sys.exit(0)
        
        # List missing folders only if requested
        if args.list_missing_folders:
            list_missing_folders_only(
                start_date=args.start_date,
                end_date=args.end_date,
                specific_dates=args.dates
            )
            sys.exit(0)
        
        # Validate date format if provided
        def validate_date(date_str):
            try:
                datetime.strptime(date_str, '%Y-%m-%d')
                return True
            except ValueError:
                return False
        
        # Validate specific dates
        if args.dates:
            invalid_dates = [d for d in args.dates if not validate_date(d)]
            if invalid_dates:
                print(f"❌ Invalid date format: {', '.join(invalid_dates)}")
                print("   Please use YYYY-MM-DD format (e.g., 2025-02-19)")
                sys.exit(1)
        
        # Validate date range
        if args.start_date and not validate_date(args.start_date):
            print(f"❌ Invalid start date format: {args.start_date}")
            print("   Please use YYYY-MM-DD format")
            sys.exit(1)
            
        if args.end_date and not validate_date(args.end_date):
            print(f"❌ Invalid end date format: {args.end_date}")
            print("   Please use YYYY-MM-DD format")
            sys.exit(1)
        
        # Check for conflicting arguments
        if args.dates and (args.start_date or args.end_date):
            print("❌ Cannot use --dates with --start-date or --end-date")
            print("   Please use either specific dates OR date range, not both")
            sys.exit(1)
        
        # Run analysis with parsed arguments
        analyze_and_save_missing_files(
            start_date=args.start_date,
            end_date=args.end_date,
            specific_dates=args.dates,
            summary_only=args.summary_only,
            output_filename=args.output
        )
        
    except KeyboardInterrupt:
        print("\n⚠️  Analysis interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error occurred: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
