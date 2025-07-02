import os
import json
import re
import datetime
from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from survey.models import FilenamePatternAnalyzer


class Command(BaseCommand):
    help = 'Analyze FITS filename patterns in observation data directories'

    def add_arguments(self, parser):
        parser.add_argument(
            '--directory',
            type=str,
            default='/lyman/data1/obsdata',
            help='Directory to analyze (default: /lyman/data1/obsdata)'
        )
        parser.add_argument(
            '--unit',
            type=str,
            help='Analyze specific unit only (e.g., 7DT01)'
        )
        parser.add_argument(
            '--units',
            type=str,
            help='Analyze multiple units: "all", "7DT01-7DT10", or "7DT01,7DT02,7DT03"'
        )
        parser.add_argument(
            '--quick',
            action='store_true',
            help='Quick analysis - show only summary statistics'
        )
        parser.add_argument(
            '--summary-only',
            action='store_true',
            help='Show only summary statistics for all units'
        )
        parser.add_argument(
            '--unlimited',
            action='store_true',
            help='Scan all files without limits'
        )
        parser.add_argument(
            '--show-all',
            action='store_true',
            help='Show all unparseable files (use with --unlimited for complete scan)'
        )
        parser.add_argument(
            '--show-all-patterns',
            action='store_true',
            help='Show detailed examples of all pattern types found'
        )
        parser.add_argument(
            '--evolution-timeline',
            action='store_true',
            help='Show how filename patterns evolved over time'
        )
        parser.add_argument(
            '--filter-analysis',
            action='store_true',
            help='Show detailed filter usage analysis'
        )
        parser.add_argument(
            '--test-unparseable',
            action='store_true',
            help='Test parsing of previously unparseable files'
        )
        parser.add_argument(
            '--debug-unparseable',
            action='store_true',
            help='🔍 Debug unparseable files - find real examples and test patterns'
        )
        parser.add_argument(
            '--show-unparseable-examples',
            type=int,
            default=20,
            help='Number of unparseable examples to show (default: 20)'
        )
        parser.add_argument(
            '--test-pattern',
            type=str,
            help='Test a specific filename against all patterns'
        )
        parser.add_argument(
            '--output-json',
            type=str,
            help='Save results to JSON file'
        )
        parser.add_argument(
            '--limit',
            type=int,
            default=50000,
            help='Limit analysis to first N files (for testing, default: 50000)'
        )
        parser.add_argument(
            '--show-all-unparseable',
            action='store_true',
            help='🚨 Show ALL unparseable files without any limits'
        )

    def handle(self, *args, **options):
        """Main command handler"""
        base_directory = options['directory']
        
        if not os.path.exists(base_directory):
            raise CommandError(f"Directory does not exist: {base_directory}")

        self.stdout.write(self.style.SUCCESS(f'🔍 Analyzing FITS filename patterns in: {base_directory}'))

        try:
            if options['test_unparseable']:
                self._test_unparseable_files(FilenamePatternAnalyzer)
                return

            if options['debug_unparseable']:
                self._debug_unparseable_files(options, FilenamePatternAnalyzer)
                return

            if options['test_pattern']:
                self._test_single_pattern(options['test_pattern'], FilenamePatternAnalyzer)
                return

            # Handle units analysis
            if options['units']:
                self._analyze_units(options['units'], base_directory, options)
            elif options['unit']:
                self._analyze_single_unit(options['unit'], base_directory, options)
            else:
                self._full_analysis(base_directory, options)

        except Exception as e:
            import traceback
            self.stdout.write(self.style.ERROR(f"Analysis failed: {e}"))
            self.stdout.write(self.style.ERROR(f"Traceback: {traceback.format_exc()}"))
            raise CommandError(f"Analysis failed: {e}")

    def _should_skip_file(self, filename):
        """
        Determine if a file should be skipped during analysis.
        
        Files to skip:
        1. Processed files (corsub*, sub*)
        2. Focus test files (*FOCUS*, *focus*)
        3. Snapshot files (*SNAPSHOT*)
        4. Master calibration files (master_*)
        5. BAD flagged files

        Files to KEEP for analysis (potential science data):
        - LTT* files (standard star observations)
        - Early temperature format files (might be valid science data)
        """
        if not filename or not isinstance(filename, str):
            return True
            
        filename_lower = filename.lower()
        
        # Skip processed files
        if filename.startswith(('corsub', 'sub')):
            return True
            
        # Skip focus-related files
        if ('focus' in filename_lower or 'FOCUS' in filename or 
            'focustest' in filename_lower or 'FOCUSTEST' in filename):
            return True

        # Skip snapshot files
        if 'snapshot' in filename_lower:
            return True
            
        # Skip master calibration files
        if filename.startswith('master_'):
            return True
            
        # Skip BAD flagged files
        if filename.startswith('BAD_'):
            return True
 
        # KEEP LTT files (standard star observations)
        if filename.startswith('LTT'):
            return False
           
        # DON'T skip files with temperature data (early test format)
        # Pattern: date_time_filter_temperature_exptime_sequence
        if re.match(r'^\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}_[a-zA-Z]+_-?\d+\.?\d*_\d+\.?\d*s_\d{4}\.fits', filename):
            return False
            
        # DON'T skip files with $$$$ temperature placeholder
        if '$$$$' in filename:
            return False
            
        return False

    def _analyze_units(self, units_spec, base_directory, options):
        """Analyze multiple units based on specification"""
        unit_list = self._parse_units_specification(units_spec, base_directory)
        
        if not unit_list:
            self.stdout.write(self.style.WARNING("No valid units found to analyze"))
            return

        total_units = len(unit_list)
        self.stdout.write(f"🔭 Found {total_units} units to analyze: {unit_list}")

        if options.get('summary_only'):
            self._analyze_units_summary_only(unit_list, base_directory, options)
        else:
            self._analyze_units_detailed(unit_list, base_directory, options)

    def _parse_units_specification(self, units_spec, base_directory):
        """Parse units specification into list of unit names"""
        unit_list = []
        
        if units_spec.lower() == 'all':
            # Find all 7DT units in directory
            for item in sorted(os.listdir(base_directory)):
                if item.startswith('7DT') and os.path.isdir(os.path.join(base_directory, item)):
                    unit_list.append(item)
        
        elif '-' in units_spec and units_spec.count('-') == 1:
            # Range specification like "7DT01-7DT10"
            try:
                start, end = units_spec.split('-')
                start_num = int(start.replace('7DT', ''))
                end_num = int(end.replace('7DT', ''))
                
                for i in range(start_num, end_num + 1):
                    unit_name = f"7DT{i:02d}"
                    unit_path = os.path.join(base_directory, unit_name)
                    if os.path.exists(unit_path):
                        unit_list.append(unit_name)
            except ValueError:
                raise CommandError(f"Invalid range specification: {units_spec}")
        
        elif ',' in units_spec:
            # Comma-separated list like "7DT01,7DT02,7DT03"
            specified_units = [u.strip() for u in units_spec.split(',')]
            for unit_name in specified_units:
                unit_path = os.path.join(base_directory, unit_name)
                if os.path.exists(unit_path):
                    unit_list.append(unit_name)
                else:
                    self.stdout.write(self.style.WARNING(f"Unit directory not found: {unit_name}"))
        
        else:
            # Single unit
            unit_path = os.path.join(base_directory, units_spec)
            if os.path.exists(unit_path):
                unit_list.append(units_spec)
            else:
                raise CommandError(f"Unit directory not found: {units_spec}")

        return unit_list

    def _analyze_units_summary_only(self, unit_list, base_directory, options):
        """Analyze multiple units showing only summary information"""
        self.stdout.write(self.style.SUCCESS('\n📊 MULTI-UNIT SUMMARY ANALYSIS'))
        self.stdout.write('=' * 80)
        
        total_stats = {
            'total_files': 0,
            'total_skipped': 0,
            'total_analyzed': 0,
            'total_unparseable': 0,
            'units_analyzed': 0,
            'pattern_totals': {},
            'date_range': {'earliest': None, 'latest': None}
        }
        
        unit_results = []
        
        for unit_name in unit_list:
            unit_path = os.path.join(base_directory, unit_name)
            
            try:
                self.stdout.write(f"🔄 Analyzing {unit_name}...")
                
                # Quick scan for this unit with filtering
                stats = self._quick_unit_scan_with_filtering(unit_path, options)
                
                unit_results.append({
                    'unit': unit_name,
                    'stats': stats
                })
                
                # Accumulate totals
                total_stats['total_files'] += stats.get('total_files_found', 0)
                total_stats['total_skipped'] += stats.get('total_files_skipped', 0)
                total_stats['total_analyzed'] += stats.get('total_files_analyzed', 0)
                total_stats['total_unparseable'] += stats.get('unparseable_count', 0)
                total_stats['units_analyzed'] += 1
                
                # Accumulate pattern counts
                pattern_counts = stats.get('pattern_counts', {})
                for pattern, count in pattern_counts.items():
                    total_stats['pattern_totals'][pattern] = total_stats['pattern_totals'].get(pattern, 0) + count
                
                # Update date range
                date_range = stats.get('date_range', {})
                if date_range.get('earliest'):
                    if not total_stats['date_range']['earliest'] or date_range['earliest'] < total_stats['date_range']['earliest']:
                        total_stats['date_range']['earliest'] = date_range['earliest']
                if date_range.get('latest'):
                    if not total_stats['date_range']['latest'] or date_range['latest'] > total_stats['date_range']['latest']:
                        total_stats['date_range']['latest'] = date_range['latest']
                
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"❌ Failed to analyze {unit_name}: {str(e)}"))

        # Display summary results
        self._display_multi_unit_summary(unit_results, total_stats)

    def _quick_unit_scan_with_filtering(self, unit_path, options):
        """Perform quick scan of a unit directory with filtering applied"""
        stats = {
            'total_files_found': 0,
            'total_files_skipped': 0,
            'total_files_analyzed': 0,
            'unparseable_count': 0,
            'pattern_counts': {},
            'skipped_counts': {'processed': 0, 'focus': 0, 'snapshot': 0, 'master': 0, 'bad': 0, 'temperature': 0},
            'date_range': {'earliest': None, 'latest': None}
        }
        
        # Set scan limits
        unlimited = options.get('unlimited', False)
        limit = float('inf') if unlimited else options.get('limit', 5000)  # Lower default for summary
        
        file_count = 0
        
        for root, dirs, files in os.walk(unit_path):
            dirs[:] = [d for d in dirs if not d.startswith('.')]
            
            for filename in files:
                if not (filename.endswith('.fits') or filename.endswith('.fits.fz')):
                    continue
                
                stats['total_files_found'] += 1
                
                # Check if file should be skipped
                if self._should_skip_file(filename):
                    stats['total_files_skipped'] += 1
                    # Categorize skip reason
                    if filename.startswith(('corsub', 'sub')):
                        stats['skipped_counts']['processed'] += 1
                    elif 'focus' in filename.lower() or 'FOCUS' in filename:
                        stats['skipped_counts']['focus'] += 1
                    elif 'snapshot' in filename.lower():
                        stats['skipped_counts']['snapshot'] += 1
                    elif filename.startswith('master_'):
                        stats['skipped_counts']['master'] += 1
                    elif filename.startswith('BAD_'):
                        stats['skipped_counts']['bad'] += 1
                    else:
                        stats['skipped_counts']['temperature'] += 1
                    continue
                
                file_count += 1
                stats['total_files_analyzed'] += 1
                
                if not unlimited and file_count > limit:
                    break
                
                try:
                    analyzer = FilenamePatternAnalyzer(filename)
                    pattern = getattr(analyzer, 'filename_pattern', 'unknown')
                    stats['pattern_counts'][pattern] = stats['pattern_counts'].get(pattern, 0) + 1
                    
                    # Update date range
                    date_obj = getattr(analyzer, 'date', None)
                    if date_obj:
                        if not stats['date_range']['earliest'] or date_obj < stats['date_range']['earliest']:
                            stats['date_range']['earliest'] = date_obj
                        if not stats['date_range']['latest'] or date_obj > stats['date_range']['latest']:
                            stats['date_range']['latest'] = date_obj
                    
                except ValueError:
                    stats['unparseable_count'] += 1
                    stats['pattern_counts']['unparseable'] = stats['pattern_counts'].get('unparseable', 0) + 1
            
            if not unlimited and file_count > limit:
                break
        
        return stats

    def _display_multi_unit_summary(self, unit_results, total_stats):
        """Display summary results for multiple units with filtering information"""
        self.stdout.write(f"\n📈 FILTERED ANALYSIS COMPLETE")
        self.stdout.write('=' * 80)
        
        # Overall statistics
        total_files = total_stats['total_files']
        total_skipped = total_stats['total_skipped']
        total_analyzed = total_stats['total_analyzed']
        total_unparseable = total_stats['total_unparseable']
        
        if total_analyzed > 0:
            success_rate = ((total_analyzed - total_unparseable) / total_analyzed * 100)
        else:
            success_rate = 0
        
        overall_rate = ((total_files - total_unparseable) / total_files * 100) if total_files > 0 else 0
        
        self.stdout.write(f"🎯 OVERALL STATISTICS:")
        self.stdout.write(f"   • Units analyzed: {total_stats['units_analyzed']}")
        self.stdout.write(f"   • Total FITS files found: {total_files:,}")
        self.stdout.write(f"   • Files skipped (non-science): {total_skipped:,}")
        self.stdout.write(f"   • Science files analyzed: {total_analyzed:,}")
        self.stdout.write(f"   • Unparseable science files: {total_unparseable:,}")
        self.stdout.write(f"   • Science data parsing success: {success_rate:.2f}%")
        self.stdout.write(f"   • Overall effective success: {overall_rate:.2f}%")
        
        # Date range
        date_range = total_stats['date_range']
        if date_range['earliest'] and date_range['latest']:
            days = (date_range['latest'] - date_range['earliest']).days
            self.stdout.write(f"   • Date range: {date_range['earliest']} to {date_range['latest']} ({days} days)")
        
        # Pattern distribution
        self.stdout.write(f"\n📋 SCIENCE DATA PATTERN DISTRIBUTION:")
        sorted_patterns = sorted(total_stats['pattern_totals'].items(), key=lambda x: x[1], reverse=True)
        for pattern, count in sorted_patterns:
            if count > 0 and pattern != 'unparseable':
                percentage = (count / total_analyzed * 100) if total_analyzed > 0 else 0
                pattern_name = self._get_pattern_display_name(pattern)
                self.stdout.write(f"   • {pattern_name}: {count:,} files ({percentage:.1f}%)")
        
        # Show unparseable if any
        if total_unparseable > 0:
            percentage = (total_unparseable / total_analyzed * 100) if total_analyzed > 0 else 0
            self.stdout.write(f"   • Unparseable: {total_unparseable:,} files ({percentage:.1f}%)")
        
        # Per-unit breakdown
        self.stdout.write(f"\n📊 PER-UNIT BREAKDOWN:")
        for result in unit_results:
            unit_name = result['unit']
            stats = result['stats']
            unit_found = stats['total_files_found']
            unit_skipped = stats['total_files_skipped']
            unit_analyzed = stats['total_files_analyzed']
            unit_unparseable = stats['unparseable_count']
            
            if unit_analyzed > 0:
                unit_success = ((unit_analyzed - unit_unparseable) / unit_analyzed * 100)
            else:
                unit_success = 0
            
            status_icon = "✅" if unit_success >= 99.0 else "⚠️" if unit_success >= 95.0 else "❌"
            self.stdout.write(f"   {status_icon} {unit_name}: {unit_found:,} total, {unit_skipped:,} skipped, {unit_analyzed:,} analyzed, {unit_success:.1f}% success")
            
            if unit_unparseable > 0:
                self.stdout.write(f"      📋 Unparseable: {unit_unparseable} files")
        
        # Recommendations
        if total_unparseable > 0:
            self.stdout.write(f"\n💡 RECOMMENDATIONS:")
            self.stdout.write(f"   • Use --debug-unparseable --units <unit> to analyze specific failures")
            self.stdout.write(f"   • Use --show-all --unlimited for complete unparseable file listing")
            self.stdout.write(f"   • Focus on units with highest unparseable counts first")
        else:
            self.stdout.write(f"\n🎉 EXCELLENT: All science data files are properly parsed!")
            self.stdout.write(f"   • Non-science files (focus tests, snapshots, etc.) properly filtered")
            self.stdout.write(f"   • No pattern improvements needed")

    def _analyze_units_detailed(self, unit_list, base_directory, options):
        """Analyze multiple units with detailed output"""
        for i, unit_name in enumerate(unit_list, 1):
            self.stdout.write(f"\n{'=' * 80}")
            self.stdout.write(f"📂 ANALYZING UNIT {i}/{len(unit_list)}: {unit_name}")
            self.stdout.write(f"{'=' * 80}")
            
            try:
                self._analyze_single_unit(unit_name, base_directory, options)
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Failed to analyze {unit_name}: {str(e)}"))

    def _debug_unparseable_files(self, options, AnalyzerClass):
        """🔍 Debug unparseable files by finding real examples and testing patterns"""
        self.stdout.write(self.style.SUCCESS('\n🔍 COMPREHENSIVE UNPARSEABLE FILE ANALYSIS (WITH FILTERING)'))
        
        # Get parameters with safe defaults
        base_directory = options.get('directory') or '/lyman/data1/obsdata'
        unit_name = options.get('unit')
        units_spec = options.get('units')
        show_all = options.get('show_all_unparseable', False) or options.get('show_all', False)
        unlimited = options.get('unlimited', False)
        
        # 🚨 제약 없이 모든 파일 보기 모드
        if show_all and unlimited:
            max_examples = float('inf')  # 무제한
            limit = float('inf')  # 무제한
            self.stdout.write(self.style.WARNING('🚨 SHOWING ALL UNPARSEABLE FILES (NO LIMITS)'))
        else:
            max_examples = options.get('show_unparseable_examples') or 20
            limit = options.get('limit') or 50000
            
        # 추가 안전 체크
        if limit is None or (not (show_all and unlimited) and limit == float('inf')):
            limit = 50000
        if max_examples is None or (not (show_all and unlimited) and max_examples == float('inf')):
            max_examples = 20
            
        # Determine search path based on units specification
        if units_spec:
            unit_list = self._parse_units_specification(units_spec, base_directory)
            if len(unit_list) == 1:
                search_path = os.path.join(base_directory, unit_list[0])
                unit_display = unit_list[0]
            else:
                search_path = base_directory
                unit_display = f"{len(unit_list)} units: {unit_list}"
        elif unit_name:
            search_path = os.path.join(base_directory, unit_name)
            unit_display = unit_name
        else:
            search_path = base_directory
            unit_display = "All units"
            
        if not os.path.exists(search_path):
            self.stdout.write(self.style.ERROR(f"Path does not exist: {search_path}"))
            return

        # Display analysis parameters
        self.stdout.write(f'📂 Unit: {unit_display} | Path: {search_path}')
        
        if show_all and unlimited:
            self.stdout.write('📊 Scan mode: UNLIMITED (all files will be checked)')
            self.stdout.write('📋 Display mode: ALL unparseable files will be shown')
        else:
            self.stdout.write(f'📊 Scan limit: {limit:,} files | Examples to show: {max_examples}')
        self.stdout.write('=' * 80)
        
        unparseable_files = []
        skipped_files = []
        total_fits_found = 0
        total_checked = 0
        
        # Progress reporting setup
        if show_all and unlimited:
            progress_interval = 5000  # 더 자주 보고
        else:
            progress_interval = max(1000, int(limit) // 10) if limit != float('inf') else 5000
        
        # Pattern failure categorization
        failure_categories = {
            'processed_files': [],      # corsub*, sub*
            'focus_files': [],          # *FOCUS*, *focus*
            'snapshot_files': [],       # *SNAPSHOT*
            'master_files': [],         # master_*
            'bad_files': [],           # BAD_*
            'temperature_files': [],    # early format with temperature
            'unknown_structures': []    # truly unknown
        }
        
        self.stdout.write("🔄 Scanning directory structure...")
        
        # Walk through directories to find unparseable files
        try:
            for root, dirs, files in os.walk(search_path):
                # Skip hidden directories
                dirs[:] = [d for d in dirs if not d.startswith('.')]
                
                for filename in files:
                    if not (filename.endswith('.fits') or filename.endswith('.fits.fz')):
                        continue
                        
                    total_fits_found += 1
                    
                    # Check if we should skip this file
                    if self._should_skip_file(filename):
                        skipped_files.append(filename)
                        self._categorize_skipped_file(filename, failure_categories)
                        continue
                    
                    total_checked += 1
                    
                    # Progress reporting
                    if total_checked % progress_interval == 0:
                        if show_all and unlimited:
                            self.stdout.write(f"   📊 Progress: {total_checked:,} files checked, {len(unparseable_files):,} unparseable found, {len(skipped_files):,} skipped")
                        else:
                            percentage = (total_checked / limit) * 100 if limit != float('inf') else 0
                            self.stdout.write(f"   📊 Progress: {percentage:.1f}% ({total_checked:,}/{limit:,} files)")
                    
                    # Check limit only if not showing all
                    if not (show_all and unlimited) and total_checked > limit:
                        break
                        
                    try:
                        # Attempt to parse the file
                        analyzer = AnalyzerClass(filename)
                        # Successfully parsed - continue
                        
                    except ValueError as e:
                        # This is unparseable - collect it
                        unparseable_files.append(filename)
                        
                        # Categorize the failure
                        self._categorize_true_failure(filename, failure_categories)
                        
                        # Check example limit only if not showing all
                        if not (show_all and unlimited) and len(unparseable_files) >= max_examples * 3:
                            break
                
                # Check limits only if not showing all
                if not (show_all and unlimited) and (total_checked > limit or len(unparseable_files) >= max_examples * 3):
                    break
                    
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error during directory scanning: {str(e)}"))
            return

        # Report results
        self._report_filtered_results(total_fits_found, total_checked, len(skipped_files), unparseable_files, failure_categories, show_all and unlimited)

    def _categorize_skipped_file(self, filename, categories):
        """Categorize files that were intentionally skipped."""
        if not filename:
            return
            
        filename_lower = filename.lower()
        
        if filename.startswith(('corsub', 'sub')):
            categories['processed_files'].append(filename)
        elif ('focus' in filename_lower or 'FOCUS' in filename or
              'focustest' in filename_lower or 'FOCUSTEST' in filename):
            categories['focus_files'].append(filename)
        elif 'snapshot' in filename_lower:
            categories['snapshot_files'].append(filename)
        elif filename.startswith('master_'):
            categories['master_files'].append(filename)
        elif filename.startswith('BAD_'):
            categories['bad_files'].append(filename)
        elif '$$$$' in filename or re.match(r'^\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}_[a-zA-Z]+_-?\d+\.?\d*_\d+\.?\d*s_\d{4}\.fits', filename):
            categories['temperature_files'].append(filename)

    def _categorize_true_failure(self, filename, categories):
        """Categorize files that truly failed to parse (after filtering)."""
        if not filename:
            return
            
        # These should be genuinely unknown patterns now
        categories['unknown_structures'].append(filename)

    def _report_filtered_results(self, total_fits, total_checked, total_skipped, unparseable_files, failure_categories, show_all):
        """Report results with filtering information."""
        self.stdout.write(f"\n📊 FILTERED SCAN RESULTS:")
        self.stdout.write(f"   • Total FITS files found: {total_fits:,}")
        self.stdout.write(f"   • Files skipped (known non-science): {total_skipped:,}")
        self.stdout.write(f"   • Science files analyzed: {total_checked:,}")
        self.stdout.write(f"   • True unparseable files: {len(unparseable_files):,}")
        
        if total_checked > 0:
            effective_success_rate = ((total_checked - len(unparseable_files)) / total_checked * 100)
            self.stdout.write(f"   • Science data parsing success rate: {effective_success_rate:.2f}%")
        
        if total_fits > 0:
            overall_success_rate = ((total_fits - len(unparseable_files)) / total_fits * 100)
            self.stdout.write(f"   • Overall effective success rate: {overall_success_rate:.2f}%")
        
        # Show what was skipped
        self.stdout.write(f"\n🚫 SKIPPED FILE CATEGORIES:")
        skip_categories = [
            ('processed_files', 'Processed files (corsub*, sub*)'),
            ('focus_files', 'Focus-related files'),
            ('snapshot_files', 'Snapshot files'),
            ('master_files', 'Master calibration files'),
            ('bad_files', 'BAD flagged files'),
            #('temperature_files', 'Early temperature format files')
        ]
        
        for category_key, description in skip_categories:
            count = len(failure_categories.get(category_key, []))
            if count > 0:
                self.stdout.write(f"   📁 {description}: {count} files")

        # Show true failures
        if len(unparseable_files) == 0:
            self.stdout.write(self.style.SUCCESS("\n🎉 No truly unparseable files found after filtering!"))
        else:
            self.stdout.write(f"\n❌ TRULY UNPARSEABLE FILES: {len(unparseable_files)}")
            for i, filename in enumerate(sorted(unparseable_files), 1):
                self.stdout.write(f"   {i:3d}. {filename}")

        # Summary recommendation
        if len(unparseable_files) == 0:
            self.stdout.write(f"\n✅ RECOMMENDATION: Current filtering strategy is working perfectly!")
            self.stdout.write(f"   All non-science files are properly identified and skipped.")
        else:
            self.stdout.write(f"\n🔍 RECOMMENDATION: {len(unparseable_files)} files need pattern analysis")
            self.stdout.write(f"   These might be new patterns or edge cases to handle.")

    def _test_single_pattern(self, filename, AnalyzerClass):
        """Test a specific filename against all patterns with detailed reporting."""
        self.stdout.write(self.style.SUCCESS('\n🧪 COMPREHENSIVE FILENAME TEST'))
        self.stdout.write(f'📁 Testing: {filename}')
        self.stdout.write('=' * 70)
        
        # First check if file should be skipped
        if self._should_skip_file(filename):
            self.stdout.write(self.style.WARNING("🚫 This file would be SKIPPED by filtering rules:"))
            self._categorize_and_explain_skip(filename)
            self.stdout.write("\n💡 This is expected behavior - these files are intentionally excluded from analysis.")
            return
        
        try:
            # Attempt to parse with main analyzer
            analyzer = AnalyzerClass(filename)
            
            # 안전한 속성 접근
            pattern = getattr(analyzer, 'filename_pattern', None)
            date = getattr(analyzer, 'date', None)
            parsed = getattr(analyzer, 'parsed_filename', None)
            
            self.stdout.write("✅ SUCCESS: File parsed successfully!")
            self.stdout.write(f"📋 Pattern: {pattern if pattern else 'Unknown'}")
            self.stdout.write(f"📅 Date: {date if date else 'No date found'}")
            
            if parsed:
                self.stdout.write("\n📋 PARSED DETAILS:")
                for key, value in parsed.items():
                    if value is not None:
                        self.stdout.write(f"   • {key}: {value}")
            
            # Show additional metadata
            self.stdout.write("\n🔍 METADATA:")
            
            try:
                is_science = analyzer.is_science_frame()
                self.stdout.write(f"   • Science frame: {is_science}")
            except Exception:
                self.stdout.write("   • Science frame: Unknown")
                
            try:
                exposure_time = analyzer.get_exposure_time()
                if exposure_time:
                    self.stdout.write(f"   • Exposure time: {exposure_time}s")
            except Exception:
                pass
                
            try:
                timestamp = analyzer.extract_timestamp()
                if timestamp:
                    self.stdout.write(f"   • Full timestamp: {timestamp}")
            except Exception:
                pass
                
        except ValueError as e:
            self.stdout.write(f"❌ PARSING FAILED: {str(e)}")
            self._analyze_failed_filename(filename)
            self._test_individual_patterns(filename)
            self._suggest_pattern_fix(filename)

    def _categorize_and_explain_skip(self, filename):
        """Explain why a file was skipped."""
        filename_lower = filename.lower()
        
        if filename.startswith(('corsub', 'sub')):
            self.stdout.write("   📁 Category: Processed files")
            self.stdout.write("   📝 Reason: Data reduction pipeline output (corsub=cosmic ray subtracted, sub=subtracted)")
            
        elif 'focus' in filename_lower or 'FOCUS' in filename:
            self.stdout.write("   📁 Category: Focus-related files")
            self.stdout.write("   📝 Reason: Focus testing or calibration data, not science observations")
            
        elif 'snapshot' in filename_lower:
            self.stdout.write("   📁 Category: Snapshot files")
            self.stdout.write("   📝 Reason: Quick test images or system snapshots")
            
        elif filename.startswith('master_'):
            self.stdout.write("   📁 Category: Master calibration files")
            self.stdout.write("   📝 Reason: Combined calibration frames (master bias, dark, flat)")
            
        elif filename.startswith('BAD_'):
            self.stdout.write("   📁 Category: BAD flagged files")
            self.stdout.write("   📝 Reason: Files marked as problematic or invalid")
            
    def _analyze_failed_filename(self, filename):
        """Analyze structure of failed filename to understand why it failed."""
        if not filename:
            self.stdout.write("\n🔬 STRUCTURE ANALYSIS: Invalid filename")
            return
            
        self.stdout.write("\n🔬 STRUCTURE ANALYSIS:")
        
        try:
            parts = filename.split('_')
            analysis = {
                'total_parts': len(parts),
                'starts_with_7dt': filename.startswith('7DT'),
                'starts_with_frame_type': filename.startswith(('LIGHT', 'DARK', 'BIAS', 'FLAT')),
                'has_double_underscore': '__' in filename,
                'has_old_date': bool(re.search(r'\d{4}-\d{2}-\d{2}', filename)),
                'has_new_date': bool(re.search(r'\d{8}', filename)),
                'has_focustest': 'focustest' in filename.lower(),
                'has_focus_pattern': bool(re.search(r'FOCUS\d+', filename)),
                'has_fits_extension': filename.endswith(('.fits', '.fits.fz')),
                'has_object_number': any('_' in part for part in parts[1:3]) if len(parts) > 2 else False
            }
            
            for key, value in analysis.items():
                status = "✅" if value else "❌"
                display_key = key.replace('_', ' ').title()
                self.stdout.write(f"   {status} {display_key}: {value}")
            
            # Show the parts breakdown
            self.stdout.write("\n   📝 PARTS BREAKDOWN:")
            for i, part in enumerate(parts):
                self.stdout.write(f"      [{i}] {part}")
                
        except Exception as e:
            self.stdout.write(f"   ❌ Error analyzing filename: {str(e)}")

    def _test_individual_patterns(self, filename):
        """Test filename against each pattern individually - SAFE VERSION."""
        if not filename:
            self.stdout.write("\n🧪 INDIVIDUAL PATTERN TESTS: Invalid filename")
            return
            
        self.stdout.write("\n🧪 INDIVIDUAL PATTERN TESTS:")
        
        # 실제 존재하는 패턴들만 테스트 (하드코딩으로 안전하게)
        patterns_to_test = []
        analyzer_class = FilenamePatternAnalyzer
        
        # 안전하게 패턴 확인
        pattern_names = [
            'NEW_FITS_PATTERN',
            'OLD_V2_FITS_PATTERN',
            'FLATWIZARD_PATTERN',
            'OLD_V1_FITS_PATTERN',
        #    'OLD_V1_FOCUS_CALIB_PATTERN',
            'OLD_V1_SIMPLE_PATTERN',
            'OLD_V0_FITS_PATTERN',
            'OLD_V0_CALIB_PATTERN'
        ]
        
        for pattern_name in pattern_names:
            try:
                if hasattr(analyzer_class, pattern_name):
                    pattern_obj = getattr(analyzer_class, pattern_name)
                    if hasattr(pattern_obj, 'match'):
                        patterns_to_test.append((pattern_name, pattern_obj))
            except Exception:
                continue
        
        if not patterns_to_test:
            self.stdout.write("   ⚠️  No patterns found in FilenamePatternAnalyzer")
            return
        
        for pattern_name, pattern_regex in patterns_to_test:
            try:
                match = pattern_regex.match(filename)
                if match:
                    self.stdout.write(f"   ✅ {pattern_name}: MATCHES")
                    groups = match.groups() if match.groups() else ()
                    self.stdout.write(f"      Groups ({len(groups)}): {groups}")
                else:
                    self.stdout.write(f"   ❌ {pattern_name}: no match")
                    
            except Exception as e:
                self.stdout.write(f"   ⚠️  {pattern_name}: error testing - {str(e)}")

    def _suggest_pattern_fix(self, filename):
        """Suggest specific pattern improvements based on filename structure."""
        if not filename:
            self.stdout.write("\n💡 PATTERN IMPROVEMENT SUGGESTIONS: Invalid filename")
            return
            
        self.stdout.write("\n💡 PATTERN IMPROVEMENT SUGGESTIONS:")
        
        try:
            parts = filename.split('_')
            
            # Analyze the specific patterns we found
            if len(parts) == 8 and any(part.isdigit() for part in parts[1:3] if part):
                self.stdout.write("   🎯 HIGH PRIORITY: Object+number pattern detected")
                self.stdout.write("      Structure: FRAME_OBJECT_NUMBER_DATE_TIME_FILTER_EXPTIME_SEQ")
                self.stdout.write("      This should match an OBJECT_NUMBER pattern in your model")
                
            elif len(parts) == 7 and '__' in filename:
                self.stdout.write("   🎯 HIGH PRIORITY: Empty object pattern detected")
                self.stdout.write("      Structure: FRAME__DATE_TIME_FILTER_EXPTIME_SEQ")
                self.stdout.write("      This should match an EMPTY_OBJECT pattern in your model")
                
            else:
                self.stdout.write("   🔍 ANALYSIS NEEDED: Uncommon pattern structure")
                self.stdout.write(f"      Manual review recommended for: {filename}")
                
        except Exception as e:
            self.stdout.write(f"   ❌ Error suggesting pattern fix: {str(e)}")

    def _analyze_single_unit(self, unit_name, base_directory, options):
        """Analyze a single telescope unit"""
        unit_path = os.path.join(base_directory, unit_name)
        
        if not os.path.exists(unit_path):
            raise CommandError(f"Unit directory does not exist: {unit_path}")

        self.stdout.write(f"📂 Analyzing unit: {unit_name}")
        
        try:
            # Use our own analysis method with filtering
            stats = self._analyze_unit_directory_with_filtering(unit_path, options)
            
            if options.get('quick'):
                self._show_quick_summary_with_filtering(stats, unit_name)
            else:
                self._show_detailed_results_with_filtering(stats, unit_name)

            if options.get('output_json'):
                self._save_to_json(stats, options['output_json'], unit_name)
                
        except Exception as e:
            raise CommandError(f"Failed to analyze unit {unit_name}: {e}")

    def _analyze_unit_directory_with_filtering(self, unit_path, options):
        """Analyze a unit directory with filtering and return statistics"""
        stats = {
            'total_files_found': 0,
            'total_files_skipped': 0,
            'total_files_analyzed': 0,
            'pattern_counts': {},
            'skipped_counts': {'processed': 0, 'focus': 0, 'snapshot': 0, 'master': 0, 'bad': 0, 'temperature': 0},
            'date_range': {'earliest': None, 'latest': None}
        }
        
        unlimited = options.get('unlimited', False)
        limit = float('inf') if unlimited else options.get('limit', 50000)
        
        file_count = 0
        
        for root, dirs, files in os.walk(unit_path):
            dirs[:] = [d for d in dirs if not d.startswith('.')]
            
            for filename in files:
                if not (filename.endswith('.fits') or filename.endswith('.fits.fz')):
                    continue
                
                stats['total_files_found'] += 1
                
                # Check if file should be skipped
                if self._should_skip_file(filename):
                    stats['total_files_skipped'] += 1
                    # Categorize skip reason
                    if filename.startswith(('corsub', 'sub')):
                        stats['skipped_counts']['processed'] += 1
                    elif 'focus' in filename.lower() or 'FOCUS' in filename:
                        stats['skipped_counts']['focus'] += 1
                    elif 'snapshot' in filename.lower():
                        stats['skipped_counts']['snapshot'] += 1
                    elif filename.startswith('master_'):
                        stats['skipped_counts']['master'] += 1
                    elif filename.startswith('BAD_'):
                        stats['skipped_counts']['bad'] += 1
                    else:
                        stats['skipped_counts']['temperature'] += 1
                    continue
                
                file_count += 1
                stats['total_files_analyzed'] += 1
                
                if not unlimited and file_count > limit:
                    break
                
                try:
                    analyzer = FilenamePatternAnalyzer(filename)
                    pattern = getattr(analyzer, 'filename_pattern', 'unknown')
                    stats['pattern_counts'][pattern] = stats['pattern_counts'].get(pattern, 0) + 1
                    
                    # Update date range
                    date_obj = getattr(analyzer, 'date', None)
                    if date_obj:
                        if not stats['date_range']['earliest'] or date_obj < stats['date_range']['earliest']:
                            stats['date_range']['earliest'] = date_obj
                        if not stats['date_range']['latest'] or date_obj > stats['date_range']['latest']:
                            stats['date_range']['latest'] = date_obj
                    
                except ValueError:
                    stats['pattern_counts']['unparseable'] = stats['pattern_counts'].get('unparseable', 0) + 1
            
            if not unlimited and file_count > limit:
                break
        
        return stats

    def _show_quick_summary_with_filtering(self, stats, unit_name=None):
        """Show quick summary of analysis results with filtering information"""
        title = "📊 QUICK SUMMARY (FILTERED)"
        if unit_name:
            title += f" - {unit_name}"
            
        self.stdout.write(self.style.SUCCESS(f'\n{title}'))
        self.stdout.write(self.style.SUCCESS('=' * len(title)))

        total_found = stats.get('total_files_found', 0)
        total_skipped = stats.get('total_files_skipped', 0)
        total_analyzed = stats.get('total_files_analyzed', 0)
        unparseable = stats.get('pattern_counts', {}).get('unparseable', 0)
        
        if total_analyzed > 0:
            success_rate = ((total_analyzed - unparseable) / total_analyzed * 100)
        else:
            success_rate = 0

        self.stdout.write(f"📁 Total FITS files found: {total_found}")
        self.stdout.write(f"🚫 Files skipped (non-science): {total_skipped}")
        self.stdout.write(f"📊 Science files analyzed: {total_analyzed}")
        self.stdout.write(f"✅ Successfully parsed: {total_analyzed - unparseable}")
        self.stdout.write(f"❌ Unparseable: {unparseable}")
        self.stdout.write(f"📈 Science data success rate: {success_rate:.1f}%")

        # Date range
        date_range = stats.get('date_range', {})
        earliest = date_range.get('earliest')
        latest = date_range.get('latest')
        
        if earliest and latest:
            try:
                days = (latest - earliest).days
                self.stdout.write(f"📅 Date range: {earliest} to {latest} ({days} days)")
            except (TypeError, AttributeError):
                self.stdout.write(f"📅 Date range: {earliest} to {latest}")

    def _show_detailed_results_with_filtering(self, stats, unit_name=None):
        """Show comprehensive analysis results with filtering information"""
        title = "📈 DETAILED ANALYSIS RESULTS (FILTERED)"
        if unit_name:
            title += f" - {unit_name}"
            
        self.stdout.write(self.style.SUCCESS(f'\n{title}'))
        self.stdout.write(self.style.SUCCESS('=' * len(title)))

        total_found = stats.get('total_files_found', 0)
        total_skipped = stats.get('total_files_skipped', 0)
        total_analyzed = stats.get('total_files_analyzed', 0)

        self.stdout.write(f"📁 Total FITS files found: {total_found}")
        self.stdout.write(f"🚫 Files skipped (non-science): {total_skipped}")
        
        # Show skip breakdown
        skipped_counts = stats.get('skipped_counts', {})
        if total_skipped > 0:
            self.stdout.write(f"   📋 Skipped breakdown:")
            skip_types = [
                ('processed', 'Processed files (corsub*, sub*)'),
                ('focus', 'Focus-related files'),
                ('snapshot', 'Snapshot files'),
                ('master', 'Master calibration files'),
                ('bad', 'BAD flagged files'),
                ('temperature', 'Early temperature format files')
            ]
            
            for skip_type, description in skip_types:
                count = skipped_counts.get(skip_type, 0)
                if count > 0:
                    self.stdout.write(f"      • {description}: {count}")
        
        self.stdout.write(f"📊 Science files analyzed: {total_analyzed}")

        # Date range
        date_range = stats.get('date_range', {})
        earliest = date_range.get('earliest')
        latest = date_range.get('latest')
        
        if earliest and latest:
            try:
                days = (latest - earliest).days
                self.stdout.write(f"📅 Date range: {earliest} to {latest} ({days} days)")
            except (TypeError, AttributeError):
                self.stdout.write(f"📅 Date range: {earliest} to {latest}")

        # Pattern distribution
        self.stdout.write("\n📋 Science Data Pattern Distribution:")
        pattern_counts = stats.get('pattern_counts', {})
        sorted_patterns = sorted(pattern_counts.items(), key=lambda x: x[1], reverse=True)
        
        for pattern, count in sorted_patterns:
            if count > 0:
                percentage = (count / total_analyzed * 100) if total_analyzed > 0 else 0
                pattern_name = self._get_pattern_display_name(pattern)
                self.stdout.write(f"  • {pattern_name}: {count} files ({percentage:.1f}%)")

        # Show unparseable files if they exist
        unparseable_count = pattern_counts.get('unparseable', 0)
        if unparseable_count > 0:
            self.stdout.write(f"\n❌ UNPARSEABLE SCIENCE FILES DETECTED: {unparseable_count}")
            self.stdout.write("   💡 Use --debug-unparseable to analyze these files")
            self.stdout.write("   💡 Use --debug-unparseable --show-all-unparseable to see ALL files")
            self.stdout.write("   💡 Use --test-pattern <filename> to test individual files")
        else:
            self.stdout.write(f"\n🎉 ALL SCIENCE DATA FILES SUCCESSFULLY PARSED!")

    def _get_pattern_display_name(self, pattern):
        """Get user-friendly display name for pattern"""
        pattern_names = {
            'new_fits': 'New Format (Current)',
            'old_v2_fits': 'Old Format v2 (with Unit)',
            'old_v1_fits': 'Old Format v1 (Original)',
            'old_v0_fits': 'Old Format v0 (Early)',
            'unparseable': 'Unparseable'
        }
        return pattern_names.get(pattern, pattern.replace('_', ' ').title())

    def _full_analysis(self, base_directory, options):
        """Analyze all telescope units"""
        # Discover telescope units
        units = []
        for item in sorted(os.listdir(base_directory)):
            unit_path = os.path.join(base_directory, item)
            if os.path.isdir(unit_path) and item.startswith('7DT'):
                units.append((item, unit_path))

        if not units:
            self.stdout.write(self.style.WARNING("No telescope unit directories found"))
            return

        self.stdout.write(f"🔭 Found {len(units)} telescope units: {[name for name, _ in units]}")

        # For simplicity, analyze the first unit
        if units:
            unit_name, unit_path = units[0]
            self._analyze_single_unit(unit_name, base_directory, options)

    def _test_unparseable_files(self, AnalyzerClass):
        """Test parsing of previously unparseable files"""
        self.stdout.write(self.style.SUCCESS('\n=== TESTING PREVIOUSLY UNPARSEABLE FILES ==='))
        
        test_files = [
            # FOCUSTEST patterns
            #'LIGHT_focustest_NGC1980_2023-10-12_05-03-25_m675_6.00s_0007.fits',
            # Object number patterns  
            'LIGHT_NGC1566_20_2023-10-12_03-01-14_r_60.00s_0021.fits',
            # Empty object patterns
            'DARK__2023-10-11_20-51-11_u_60.00s_0017.fits',
            # LTT files
            'LTT1020_2023-10-11_02-58-48_u_$$$$_60.00s_0000.fits',
        ]
        
        success_count = 0
        failure_count = 0
        
        for filename in test_files:
            try:
                analyzer = AnalyzerClass(filename)
                self.stdout.write(f"✅ PARSED: {filename}")
                
                pattern = getattr(analyzer, 'filename_pattern', 'Unknown')
                date = getattr(analyzer, 'date', 'No date')
                
                self.stdout.write(f"   Pattern: {pattern}")
                self.stdout.write(f"   Date: {date}")
                success_count += 1
                
            except ValueError as e:
                self.stdout.write(f"❌ FAILED: {filename}")
                self.stdout.write(f"   Error: {str(e)}")
                failure_count += 1
            
            self.stdout.write("")
        
        # Summary
        total = len(test_files)
        success_rate = (success_count / total * 100) if total > 0 else 0
        
        self.stdout.write(self.style.SUCCESS("📊 Test Results Summary:"))
        self.stdout.write(f"  • Total test files: {total}")
        self.stdout.write(f"  • Successfully parsed: {success_count}")
        self.stdout.write(f"  • Failed to parse: {failure_count}")
        self.stdout.write(f"  • Success rate: {success_rate:.1f}%")

    def _save_to_json(self, data, filename, unit_name=None):
        """Save analysis results to JSON file"""
        # Convert date objects to strings for JSON serialization
        json_data = self._prepare_json_data(data)
        
        try:
            with open(filename, 'w') as f:
                json.dump(json_data, f, indent=2, default=str)
            
            file_desc = f"for {unit_name}" if unit_name else "for all units"
            self.stdout.write(self.style.SUCCESS(f"💾 Analysis results saved to {filename} {file_desc}"))
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Failed to save JSON: {e}"))

    def _prepare_json_data(self, data):
        """Prepare data for JSON serialization by converting dates and sets"""
        if isinstance(data, dict):
            result = {}
            for key, value in data.items():
                if key == 'date_range' and isinstance(value, dict):
                    result[key] = {
                        'earliest': str(value.get('earliest')) if value.get('earliest') else None,
                        'latest': str(value.get('latest')) if value.get('latest') else None
                    }
                elif isinstance(value, set):
                    result[key] = sorted(list(value))
                elif isinstance(value, (list, dict)):
                    result[key] = self._prepare_json_data(value)
                else:
                    result[key] = value
            return result
        elif isinstance(data, list):
            return [self._prepare_json_data(item) for item in data]
        else:
            return data
