# 🌌 GW Portal - 7DT Survey Data Management System

**Automated Data Ingestion and Management System for 7 Degree Telescope (7DT) Astronomical Observations**

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://python.org)
[![Django](https://img.shields.io/badge/Django-4.0+-green.svg)](https://djangoproject.com)
[![License](https://img.shields.io/badge/License-Private-red.svg)](LICENSE)

## 📋 Overview

GW Portal is a Django-based system designed for fully automated ingestion, processing, and management of 7-Dimentional Telescope (7DT) observational data. 

## ✨ Key Features

### 🚀 Full Automation
- **Automatic New Data Detection**: Scans for new observation folders every 8 hours
- **Smart Ingestion**: Safe data collection after file transfer completion
- **Unattended Operation**: 24/7 continuous monitoring and automatic processing

### 📊 Monitoring & Statistics
- **Real-time Monitoring**: Processing progress and system status
- **Performance Analysis**: Processing speed, success rate, resource usage
- **History Tracking**: Complete processing history and error logs

## 📁 Project Structure

```
gwportal/
├── 📂 survey/                    # Main Django app
│   ├── ingest_all_nights.py     # Batch processing of observation data
│   ├── update_nights.py         # Auto-detection and ingestion trigger
│   └── models.py                # Database models
├── 📄 CHECK_missing_files.py    # Missing file analysis
├── 📄 reprocess_missing_folders.sh  # Reprocessing problematic data
├── 📄 SIMPLE_raw_ingest.py      # Simple ingestion script
├── 📂 logs/                     # All processing logs
├── 📄 manage.py                 # Django management script
└── 📄 README.md                 # This file
```

## 🛠️ Core Scripts Details

### 🔄 Automation Workflow

#### `survey/update_nights.py`
Core script responsible for automatic detection and ingestion trigger of new observation data

**Key Features:**
- 8-hour interval smart monitoring
- File stability checks (transfer completion verification)
- System resource monitoring
- State save/restore
- Automatic statistics updates

#### `survey/ingest_all_nights.py`
Handles efficient batch processing of large-scale observation data

**Key Features:**
- Process only new data after 2025-06-29 (default)
- Automatic parallel processing (for 100k+ files)
- Automatic Target object creation
- Automatic exclusion of Focus/Test files
- Detailed progress and statistics reporting

### 🔍 Data Validation and Reprocessing

#### `CHECK_missing_files.py`
Analyzes discrepancies between database and actual file system

**Analysis Features:**
- Folder-by-folder missing file analysis
- Date range statistics
- Quick list mode
- Detailed report generation

#### `reprocess_missing_folders.sh`
Reprocesses observation data for dates with identified issues

**Features:**
- Hard-coded list of problematic dates
- Full telescope unit reprocessing
- Safe batch execution

## 🚀 Usage

### 💫 Full Automation Mode (Recommended)

```bash
# After activating virtual environment
python manage.py update_nights --auto-ingest --smart-interval --update-stats > logs/monitor.log 2>&1 &
```

**Operations performed by this single command:**
- 🕐 **8-hour intervals** for new data monitoring
- 🔍 **Auto-detection** of new observation folders
- ⏳ **Stability check** waiting for file transfer completion
- 🚀 **Auto-ingestion** immediate processing when new folders are found
- 📊 **Resource monitoring** CPU/memory usage checks
- 📈 **Statistics updates** automatic processing result updates
- 💾 **State persistence** safe restart support

### 🛠️ Manual Data Processing

#### Process only new data (after 2025-06-30)
```bash
python manage.py ingest_all_nights --new-data-only --auto-confirm
```

#### Process specific date range
```bash
python manage.py ingest_all_nights --start-date 2025-07-01 --end-date 2025-07-02 --auto-confirm
```

#### Advanced options
```bash
# Force parallel processing (8 workers)
python manage.py ingest_all_nights --parallel --workers 8 --new-data-only

# Debug mode with detailed logging
python manage.py ingest_all_nights --new-data-only --debug

# Clean existing data before reprocessing
python manage.py ingest_all_nights --cleanup --auto-confirm --start-date 2025-07-01
```

### 🔍 Data Validation and Analysis

#### Complete missing file analysis
```bash
python CHECK_missing_files.py
```

#### Quick folder analysis
```bash
python CHECK_missing_files.py --quick-folders
```

#### Specific date range analysis
```bash
python CHECK_missing_files.py --start-date 2025-07-01 --end-date 2025-07-02
```

#### Reprocess problematic dates
```bash
chmod +x reprocess_missing_folders.sh
./reprocess_missing_folders.sh
```


## 📊 Data Structure

```
/lyman/data1/obsdata/          # Base data path
├── 7DT-01/                   # Telescope unit 1
│   ├── 2025-06-30/          # Observation date
│   │   ├── *.fits           # FITS files
│   │   └── ...
│   ├── 2025-07-01/
│   └── 2025-07-02/
├── 7DT-02/                   # Telescope unit 2
├── 7DT-03/
└── ...
```

**Supported File Types:**
- **Science Frames**: Scientific observation data
- **Bias Frames**: Bias correction frames
- **Dark Frames**: Dark correction frames
- **Flat Frames**: Flat correction frames

## 📝 Logging and Monitoring

### Log File Structure
```
logs/
├── monitor.log                    # Main automation log
├── ingest_YYYYMMDD_HHMMSS.log    # Individual ingestion logs
├── missing_files_analysis_*.log   # Analysis result logs
└── validate_folders.log          # Validation logs
```

## 📞 Support and Contact

- **Project**: 7DT Survey Data Management
- **Version**: 2.0
- **Last Updated**: July 2025

---

## 📄 License

This project is managed as a Private Repository and is for internal use within the 7DT project only.

**© 2025 7DT Project. All rights reserved.**

