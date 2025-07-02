# 🌌 GW Portal - 7DT Survey Data Management System

**Automated Data Ingestion and Management System for 7-Dimensional Telescope (7DT) Astronomical Observations**

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://python.org)
[![Django](https://img.shields.io/badge/Django-4.0+-green.svg)](https://djangoproject.com)
[![License](https://img.shields.io/badge/License-Private-red.svg)](LICENSE)

## 📋 Overview

GW Portal is a Django-based system designed for fully automated ingestion, processing, and management of 7-Dimensional Telescope (7DT) observational data. 

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

