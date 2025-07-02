#!/bin/bash

# Script to reprocess specific dates that have missing files
# Currently hardcoded to process 21 dates from February-March and June 2025
# Processes entire dates (all telescopes) instead of individual telescope filtering
# This avoids the cleanup issue where telescope-specific processing deletes other telescope data

echo "🔄 Reprocessing Dates with Missing Files"
echo "========================================"
echo "⏰ Started at: $(date '+%Y-%m-%d %H:%M:%S')"

# Create logs directory if it doesn't exist
mkdir -p logs

# Hardcoded list of specific dates to reprocess
echo "🔍 Using hardcoded list of specific dates to reprocess..."

# Specific dates that need reprocessing
declare -a missing_dates=(
    "2025-02-19"
    "2025-06-28"
    "2025-06-29"
    "2025-02-13"
    "2025-02-14"
    "2025-02-17"
    "2025-02-18"
    "2025-02-20"
    "2025-02-21"
    "2025-02-22"
    "2025-02-23"
    "2025-02-24"
    "2025-02-25"
    "2025-02-26"
    "2025-02-27"
    "2025-02-28"
    "2025-03-01"
    "2025-03-02"
    "2025-03-03"
    "2025-03-04"
    "2025-03-05"
)

if [ ${#missing_dates[@]} -eq 0 ]; then
    echo "✅ No missing dates found! All data appears to be properly ingested."
    exit 0
fi

echo "📋 Detected ${#missing_dates[@]} unique dates with missing files:"
for date in "${missing_dates[@]}"; do
    echo "   📅 $date"
done

total_dates=${#missing_dates[@]}
current_date=0
success_count=0
error_count=0

echo "� Total dates to reprocess: $total_dates"
echo "🔧 Processing mode: Full date processing (all telescopes)"
echo "⚠️  Note: Using --cleanup to ensure clean re-import"
echo "========================================"

for date in "${missing_dates[@]}"; do
    current_date=$((current_date + 1))
    
    echo ""
    echo "� [$current_date/$total_dates] Processing date: $date"
    echo "⏰ Started at: $(date '+%H:%M:%S')"
    
    # Create log filename
    log_file="logs/reprocess_full_${date//-/}.log"
    
    # Check if any data exists for this date first
    data_path="/lyman/data1/obsdata/7DT*/*${date}*"
    if ! ls $data_path >/dev/null 2>&1; then
        echo "⚠️  Warning: No data directories found for date $date"
        echo "   Skipping..."
        continue
    fi
    
    echo "🚀 Running full date ingestion (all telescopes)..."
    echo "   Log file: $log_file"
    
    # Run ingestion for the entire date (all telescopes)
    python manage.py ingest_all_nights \
        --start-date $date \
        --end-date $date \
        --cleanup \
        --auto-confirm \
        --continue-on-error \
        --debug \
        --parallel \
        --workers 6 \
        --validate \
        --create-targets \
        --exclude-focus \
        --exclude-test \
        --report-interval 10 \
        2>&1 | tee "$log_file"
    
    exit_code=$?
    
    if [ $exit_code -eq 0 ]; then
        echo "✅ Date $date completed successfully at $(date '+%H:%M:%S')"
        success_count=$((success_count + 1))
    else
        echo "❌ Date $date failed at $(date '+%H:%M:%S') (exit code: $exit_code)"
        error_count=$((error_count + 1))
    fi
    
    echo "📊 Progress: $success_count success, $error_count errors, $current_date of $total_dates completed"
    
    # Show estimated time remaining
    if [ $current_date -gt 0 ]; then
        elapsed_minutes=$(( ($(date +%s) - $(date -d "$(head -1 "$log_file" 2>/dev/null | grep -o '[0-9][0-9]:[0-9][0-9]:[0-9][0-9]' || echo "00:00:00")" +%s)) / 60 ))
        avg_time_per_date=$(( elapsed_minutes / current_date ))
        remaining_dates=$(( total_dates - current_date ))
        eta_minutes=$(( remaining_dates * avg_time_per_date ))
        
        if [ $eta_minutes -gt 0 ]; then
            echo "⏱️  Estimated time remaining: ${eta_minutes} minutes"
        fi
    fi
    
    echo "=========================================="
done

echo ""
echo "🎉 Date reprocessing completed at $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================"
echo "📊 Final Results:"
echo "   ✅ Successful dates: $success_count"
echo "   ❌ Failed dates: $error_count"
echo "   � Total processed: $current_date dates"

if [ $error_count -gt 0 ]; then
    echo ""
    echo "⚠️  Some dates failed. Check the individual log files:"
    echo "   ls logs/reprocess_full_*.log"
    echo "   You can rerun specific failed dates by editing this script"
fi

echo ""
echo "💡 Next steps:"
echo "   1. Run: python CHECK_missing_files.py --list-missing-folders"
echo "   2. Verify that the missing folder count has significantly decreased"
echo "   3. If there are still missing folders, check the log files for specific issues"
echo ""
echo "📈 Expected results:"
echo "   • Should process all telescopes for each date"
echo "   • Should resolve the telescope-specific missing file issues"
echo "   • Should maintain data consistency across all telescope units"

