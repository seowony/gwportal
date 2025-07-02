from django.contrib import admin
from django.utils.html import format_html
from django.urls import reverse
from django.db.models import Count, Sum, Avg, Q
from .models import (
    Night, Tile, Target, ScienceFrame, BiasFrame, DarkFrame, FlatFrame,
    UnitStatistics
)

@admin.register(Night)
class NightAdmin(admin.ModelAdmin):
    list_display = ['date', 'science_count', 'bias_count', 'dark_count', 'flat_count', 'distinct_tiles']
    list_filter = ['date']
    search_fields = ['date']
    readonly_fields = ['created_at', 'updated_at', 'scan_started_at', 'scan_completed_at']
    
    fieldsets = [
        ('Basic Information', {
            'fields': ['date', 'sky_quality', 'notes']
        }),
        ('Astronomical Data', {
            'fields': ['sunset', 'sunrise', 'evening_twilight_end', 'morning_twilight_start',
                      'moon_phase', 'moon_illumination', 'moon_alt_max']
        }),
        ('Statistics', {
            'fields': ['science_count', 'bias_count', 'dark_count', 'flat_count', 
                      'distinct_tiles', 'total_exptime'],
            'classes': ['collapse']
        }),
        ('File System', {
            'fields': ['data_directory', 'data_directories', 'directory_variants'],
            'classes': ['collapse']
        }),
        ('Timestamps', {
            'fields': ['created_at', 'updated_at', 'scan_started_at', 'scan_completed_at'],
            'classes': ['collapse']
        })
    ]

@admin.register(Tile)
class TileAdmin(admin.ModelAdmin):
    list_display = [
        'name', 'ra_display', 'dec_display', 'observation_count_display', 
        'total_exposure_time_display', 'first_observed', 'last_observed'
    ]
    list_filter = ['first_observed', 'last_observed', 'priority']
    search_fields = ['name', 'id']
    
    actions = ['update_tile_statistics', 'recalculate_areas', 'debug_tile_connections']
    
    fieldsets = [
        ('Basic Information', {
            'fields': ['name', 'id', 'priority']
        }),
        ('Coordinates', {
            'fields': ['ra', 'dec', 'vertices']
        }),
        ('Statistics', {
            'fields': ['observation_count', 'total_exposure_time', 'area_sq_deg',
                      'first_observed', 'last_observed'],
            'classes': ['collapse']
        })
    ]
    
    readonly_fields = ['observation_count', 'total_exposure_time', 'area_sq_deg',
                      'first_observed', 'last_observed']
    
    def ra_display(self, obj):
        """Display RA in degrees with precision."""
        if obj.ra is not None:
            return f"{obj.ra:.6f}°"
        return "—"
    ra_display.short_description = "RA"
    
    def dec_display(self, obj):
        """Display Dec in degrees with precision."""
        if obj.dec is not None:
            return f"{obj.dec:.6f}°"
        return "—"
    dec_display.short_description = "Dec"
    
    def observation_count_display(self, obj):
        """Display observation count with debugging info."""
        actual_count = obj.observation_count or 0
        
        name_count = ScienceFrame.objects.filter(object_name=obj.name).count()
        
        if actual_count == 0 and name_count > 0:
            return f"🔴 {actual_count} ({name_count})" 
        elif actual_count == 0:
            return f"❌ {actual_count}"
        elif actual_count < 10:
            return f"⚠️ {actual_count}"
        else:
            return f"✅ {actual_count}"
    observation_count_display.short_description = "Observations"
    
    def total_exposure_time_display(self, obj):
        """Display total exposure time in readable format."""
        if obj.total_exposure_time and obj.total_exposure_time > 0:
            hours = obj.total_exposure_time / 3600
            if hours >= 1:
                return f"{hours:.1f}h"
            elif obj.total_exposure_time >= 60:
                return f"{obj.total_exposure_time/60:.1f}m"
            else:
                return f"{obj.total_exposure_time:.0f}s"
        return "0s"
    total_exposure_time_display.short_description = "Total ExpTime"
    
    def area_display(self, obj):
        """Display area in square degrees."""
        if obj.area_sq_deg:
            return f"{obj.area_sq_deg:.4f} sq°"
        return "—"
    area_display.short_description = "Area"
    
    def update_tile_statistics(self, request, queryset):
        """Update statistics for selected tiles."""
        updated_count = 0
        total_observations = 0
        
        for tile in queryset:
            try:
                stats = tile.update_observation_statistics()
                updated_count += 1
                total_observations += stats['observation_count']
            except Exception as e:
                self.message_user(
                    request,
                    f"❌ Error updating {tile.name}: {e}",
                    level='ERROR'
                )
        
        self.message_user(
            request,
            f"✅ Updated statistics for {updated_count} tiles. "
            f"Total observations found: {total_observations}"
        )
    update_tile_statistics.short_description = "Update observation statistics"
    
    def debug_tile_connections(self, request, queryset):
        """Debug tile connections for selected tiles."""
        debug_info = []
        
        for tile in queryset:
            connected_frames = ScienceFrame.objects.filter(tile=tile).count()
            
            name_frames = ScienceFrame.objects.filter(object_name=tile.name).count()
            
            alt_formats = [
                f"T{int(tile.name[1:]):d}",  # T25344 -> T25344
                f"T{int(tile.name[1:]):05d}",  # T25344 -> T25344
            ]
            
            alt_counts = {}
            for alt_format in alt_formats:
                if alt_format != tile.name:
                    alt_counts[alt_format] = ScienceFrame.objects.filter(
                        object_name=alt_format
                    ).count()
            
            debug_info.append(
                f"{tile.name}: Connected={connected_frames}, "
                f"ByName={name_frames}, "
                f"Alt={alt_counts}"
            )
        
        self.message_user(
            request,
            f"🔍 Debug info: {'; '.join(debug_info)}"
        )
    debug_tile_connections.short_description = "Debug tile connections"
    
    def recalculate_areas(self, request, queryset):
        """Recalculate areas for selected tiles."""
        updated_count = 0
        
        for tile in queryset:
            try:
                tile.calculate_area()
                updated_count += 1
            except Exception as e:
                self.message_user(
                    request,
                    f"❌ Error calculating area for {tile.name}: {e}",
                    level='ERROR'
                )
        
        self.message_user(
            request,
            f"✅ Recalculated areas for {updated_count} tiles."
        )
    recalculate_areas.short_description = "Recalculate areas"
    
    def get_queryset(self, request):
        """Optimize queries with related data."""
        return super().get_queryset(request).prefetch_related('scienceframe_set')

@admin.register(Target)
class TargetAdmin(admin.ModelAdmin):
    list_display = [
        'name', 'target_type', 'ra_display', 'dec_display', 
        'observation_count_display', 'first_observed_display', 'last_observed_display'
    ]
    list_filter = ['target_type', 'observation_strategy', 'first_observed', 'last_observed']
    search_fields = ['name', 'description']
    readonly_fields = ['area_sq_deg']
    
    fieldsets = [
        ('Basic Information', {
            'fields': ['name', 'target_type', 'observation_strategy', 'description']
        }),
        ('Coordinates & FOV', {
            'fields': ['ra', 'dec', 'fov_width', 'fov_height', 'position_angle']
        }),
        ('Polygon Data', {
            'fields': ['vertices', 'area_sq_deg'],
            'classes': ['collapse']
        })
    ]
    
    actions = ['update_statistics', 'regenerate_polygons']
    
    def ra_display(self, obj):
        """Display RA in degrees"""
        if obj.ra is not None:
            return f"{obj.ra:.6f}°"
        return "-"
    ra_display.short_description = "RA"
    
    def dec_display(self, obj):
        """Display Dec in degrees"""
        if obj.dec is not None:
            return f"{obj.dec:.6f}°"
        return "-"
    dec_display.short_description = "Dec"
    
    def fov_display(self, obj):
        """Display FOV dimensions"""
        if obj.fov_width and obj.fov_height:
            return f"{obj.fov_width:.3f}° × {obj.fov_height:.3f}°"
        return "-"
    fov_display.short_description = "FOV (W×H)"
    
    def observation_count_display(self, obj):
        """Count of science frames for this target"""
        count = obj.scienceframe_set.count()
        if count > 0:
            total_exp = obj.scienceframe_set.aggregate(
                total=Sum('exptime')
            )['total'] or 0
            return f"{count} ({total_exp:.0f}s)"
        return "0"
    observation_count_display.short_description = "Observations (ExpTime)"
    
    def area_display(self, obj):
        """Display area in square degrees"""
        if obj.area_sq_deg:
            return f"{obj.area_sq_deg:.6f} sq°"
        return "-"
    area_display.short_description = "Area"
    
    def update_statistics(self, request, queryset):
        """Admin action to update target statistics."""
        updated = 0
        for target in queryset:
            if hasattr(target, 'update_observation_statistics'):
                target.update_observation_statistics()
                updated += 1
        
        self.message_user(
            request, 
            f"Updated statistics for {updated} targets."
        )
    update_statistics.short_description = "Update statistics"
    
    def regenerate_polygons(self, request, queryset):
        """Admin action to regenerate polygons for targets."""
        updated = 0
        for target in queryset:
            if target.fov_width and target.fov_height and target.ra is not None and target.dec is not None:
                if hasattr(target, 'create_rectangular_polygon'):
                    vertices = target.create_rectangular_polygon()
                    if vertices:
                        import json
                        target.vertices = json.dumps(vertices)
                        target.save()
                        updated += 1
        
        self.message_user(
            request,
            f"Regenerated polygons for {updated}/{queryset.count()} targets."
        )
    regenerate_polygons.short_description = "Regenerate polygons"
    
    def get_queryset(self, request):
        """Optimize queries"""
        return super().get_queryset(request).prefetch_related('scienceframe_set')

    def first_observed_display(self, obj):
        if obj.first_observed:
            return obj.first_observed.strftime('%Y-%m-%d')
        return "-"
    first_observed_display.short_description = "First Observed"

    def last_observed_display(self, obj):
        if obj.last_observed:
            return obj.last_observed.strftime('%Y-%m-%d')
        return "-"
    last_observed_display.short_description = "Last Observed"


@admin.register(BiasFrame)
class BiasFrameAdmin(admin.ModelAdmin):
    list_display = [
        'original_filename', 'unified_filename_display', 'unit', 'night', 
        'obstime_display', 'mjd_display', 'exptime', 'gain', 'is_usable'
    ]
    list_filter = ['unit', 'night__date', 'gain', 'is_usable', 'header_parsed']
    search_fields = ['original_filename', 'unified_filename', 'image_id']
    readonly_fields = [
        'original_filename', 'unified_filename', 'file_path', 'file_size',
        'obstime', 'mjd', 'jd', 'image_id', 'created_at', 'updated_at'
    ]
    
    fieldsets = [
        ('📁 File Information', {
            'fields': ['original_filename', 'unified_filename', 'file_path', 'file_size', 'image_id']
        }),
        ('🕐 Time Information', {
            'fields': ['obstime', 'local_obstime', 'jd', 'mjd'],
            'description': 'Observation timing details'
        }),
        ('🏗️ Observation Data', {
            'fields': ['unit', 'night', 'exptime', 'gain', 'binning_x', 'binning_y']
        }),
        ('🔧 Technical Details', {
            'fields': ['instrument', 'ccdtemp', 'set_ccdtemp', 'cooler_power', 'software_used'],
            'classes': ['collapse']
        }),
        ('📊 Analysis Results', {
            'fields': ['median_level', 'noise_level', 'std_deviation', 'is_usable', 'quality_score'],
            'classes': ['collapse']
        }),
        ('🔄 Processing', {
            'fields': ['header_parsed', 'processing_status', 'created_at', 'updated_at'],
            'classes': ['collapse']
        })
    ]
    
    # Custom display methods
    def unified_filename_display(self, obj):
        """Display unified filename with truncation."""
        if obj.unified_filename:
            if len(obj.unified_filename) > 30:
                return f"{obj.unified_filename[:30]}..."
            return obj.unified_filename
        return "—"
    unified_filename_display.short_description = 'Unified Filename'
    
    def obstime_display(self, obj):
        """Display observation time in readable format."""
        if obj.obstime:
            return obj.obstime.strftime('%Y-%m-%d %H:%M:%S')
        return "—"
    obstime_display.short_description = 'Obs Time'
    
    def mjd_display(self, obj):
        """Display MJD with appropriate precision."""
        if obj.mjd:
            return f"{obj.mjd:.6f}"
        return "—"
    mjd_display.short_description = 'MJD'
    
    list_per_page = 50


@admin.register(DarkFrame)
class DarkFrameAdmin(admin.ModelAdmin):
    list_display = [
        'original_filename', 'unified_filename_display', 'unit', 'night',
        'obstime_display', 'mjd_display', 'exptime', 'gain', 'ccdtemp_display', 'is_usable'
    ]
    list_filter = ['unit', 'night__date', 'exptime', 'gain', 'is_usable', 'header_parsed']
    search_fields = ['original_filename', 'unified_filename', 'image_id']
    readonly_fields = [
        'original_filename', 'unified_filename', 'file_path', 'file_size',
        'obstime', 'mjd', 'jd', 'image_id', 'created_at', 'updated_at'
    ]
    
    fieldsets = [
        ('📁 File Information', {
            'fields': ['original_filename', 'unified_filename', 'file_path', 'file_size', 'image_id']
        }),
        ('🕐 Time Information', {
            'fields': ['obstime', 'local_obstime', 'jd', 'mjd'],
            'description': 'Observation timing details'
        }),
        ('🏗️ Observation Data', {
            'fields': ['unit', 'night', 'exptime', 'gain', 'binning_x', 'binning_y', 'ccdtemp']
        }),
        ('🔧 Technical Details', {
            'fields': ['instrument', 'set_ccdtemp', 'cooler_power', 'software_used'],
            'classes': ['collapse']
        }),
        ('📊 Analysis Results', {
            'fields': ['dark_current', 'hotpix_count', 'median_level', 'is_usable', 'quality_score'],
            'classes': ['collapse']
        }),
        ('🔄 Processing', {
            'fields': ['header_parsed', 'processing_status', 'created_at', 'updated_at'],
            'classes': ['collapse']
        })
    ]
    
    # Custom display methods
    def unified_filename_display(self, obj):
        """Display unified filename with truncation."""
        if obj.unified_filename:
            if len(obj.unified_filename) > 30:
                return f"{obj.unified_filename[:30]}..."
            return obj.unified_filename
        return "—"
    unified_filename_display.short_description = 'Unified Filename'
    
    def obstime_display(self, obj):
        """Display observation time in readable format."""
        if obj.obstime:
            return obj.obstime.strftime('%Y-%m-%d %H:%M:%S')
        return "—"
    obstime_display.short_description = 'Obs Time'
    
    def mjd_display(self, obj):
        """Display MJD with appropriate precision."""
        if obj.mjd:
            return f"{obj.mjd:.6f}"
        return "—"
    mjd_display.short_description = 'MJD'
    
    def ccdtemp_display(self, obj):
        """Display CCD temperature with units."""
        if obj.ccdtemp is not None:
            return f"{obj.ccdtemp:.1f}°C"
        return "—"
    ccdtemp_display.short_description = 'CCD Temp'
    
    list_per_page = 50


@admin.register(FlatFrame)
class FlatFrameAdmin(admin.ModelAdmin):
    list_display = [
        'original_filename', 'unified_filename_display', 'unit', 'night', 'filter',
        'obstime_display', 'mjd_display', 'exptime', 'gain', 'uniformity_display', 'is_usable'
    ]
    list_filter = ['unit', 'night__date', 'filter', 'gain', 'is_usable', 'header_parsed']
    search_fields = ['original_filename', 'unified_filename', 'image_id']
    readonly_fields = [
        'original_filename', 'unified_filename', 'file_path', 'file_size',
        'obstime', 'mjd', 'jd', 'image_id', 'created_at', 'updated_at'
    ]
    
    fieldsets = [
        ('📁 File Information', {
            'fields': ['original_filename', 'unified_filename', 'file_path', 'file_size', 'image_id']
        }),
        ('🕐 Time Information', {
            'fields': ['obstime', 'local_obstime', 'jd', 'mjd'],
            'description': 'Observation timing details'
        }),
        ('🏗️ Observation Data', {
            'fields': ['unit', 'night', 'filter', 'exptime', 'gain', 'binning_x', 'binning_y']
        }),
        ('🔧 Technical Details', {
            'fields': ['instrument', 'ccdtemp', 'set_ccdtemp', 'cooler_power', 'software_used'],
            'classes': ['collapse']
        }),
        ('📊 Analysis Results', {
            'fields': ['median_counts', 'uniformity_rms', 'vignetting_level', 'is_usable', 'quality_score'],
            'classes': ['collapse']
        }),
        ('🔄 Processing', {
            'fields': ['header_parsed', 'processing_status', 'created_at', 'updated_at'],
            'classes': ['collapse']
        })
    ]
    
    # Custom display methods
    def unified_filename_display(self, obj):
        """Display unified filename with truncation."""
        if obj.unified_filename:
            if len(obj.unified_filename) > 30:
                return f"{obj.unified_filename[:30]}..."
            return obj.unified_filename
        return "—"
    unified_filename_display.short_description = 'Unified Filename'
    
    def obstime_display(self, obj):
        """Display observation time in readable format."""
        if obj.obstime:
            return obj.obstime.strftime('%Y-%m-%d %H:%M:%S')
        return "—"
    obstime_display.short_description = 'Obs Time'
    
    def mjd_display(self, obj):
        """Display MJD with appropriate precision."""
        if obj.mjd:
            return f"{obj.mjd:.6f}"
        return "—"
    mjd_display.short_description = 'MJD'
    
    def uniformity_display(self, obj):
        """Display uniformity with quality indicator."""
        if obj.uniformity_rms is not None:
            if obj.uniformity_rms <= 0.02:
                return f"✅ {obj.uniformity_rms:.3f}"
            elif obj.uniformity_rms <= 0.05:
                return f"⚠️ {obj.uniformity_rms:.3f}"
            else:
                return f"❌ {obj.uniformity_rms:.3f}"
        return "—"
    uniformity_display.short_description = 'Uniformity RMS'
    
    list_per_page = 50

@admin.register(UnitStatistics)
class UnitStatisticsAdmin(admin.ModelAdmin):
    list_display = ['unit', 'science_frame_count', 'distinct_tiles_observed', 'total_exptime', 'last_updated']
    list_filter = ['unit']
    readonly_fields = ['last_updated']
    
    fieldsets = [
        ('Basic Information', {
            'fields': ['unit', 'first_observation', 'last_observation']
        }),
        ('Frame Counts', {
            'fields': ['science_frame_count', 'bias_frame_count', 'dark_frame_count', 'flat_frame_count']
        }),
        ('Statistics', {
            'fields': ['total_exptime', 'distinct_tiles_observed', 'distinct_nights', 'last_updated']
        })
    ]


@admin.register(ScienceFrame)
class ScienceFrameAdmin(admin.ModelAdmin):
    list_display = [
        'original_filename', 'unified_filename_display', 'unit', 'night', 'object_name', 
        'filter', 'exptime', 'obstime_display', 'mjd_display', 'software_used', 'fwhm', 'airmass'
    ]
    
    list_filter = [
        'unit', 'night', 'filter', 'software_used', 'object_type', 
        'obsmode', 'is_too', 'header_parsed'
    ]
    
    search_fields = [
        'original_filename', 'unified_filename', 'object_name', 'object_id', 'observer'
    ]
    
    readonly_fields = [
        'original_filename', 'unified_filename', 'file_path', 'file_size', 'obstime', 
        'mjd', 'jd', 'image_id', 'created_at', 'updated_at'
    ]
    
    fieldsets = (
        ('📁 File Information', {
            'fields': (
                'original_filename', 'unified_filename', 'file_path', 'file_size',
#                'filename_pattern', 'filename_metadata'
            )
        }),
        
        ('🏗️ Basic Frame Data', {
            'fields': (
                'unit', 'night', 'obstime', 'local_obstime', 'exptime',
                'binning_x', 'binning_y', 'gain', 'egain'
            )
        }),
        
        ('🎯 Observation Target', {
            'fields': (
                'object_name', 'tile', 'target', 'filter'
            ),
            'description': 'Primary target information (common to both TCSpy and NINA)'
        }),
        
        ('📍 Target Details', {
            'fields': (
                'object_type', 'object_id',
                ('object_ra_hms', 'object_dec_dms'),
                ('object_ra', 'object_dec'),
                ('object_alt', 'object_az'),
                'object_ha'
            ),
            'description': 'Detailed target information from FITS header',
            'classes': ('collapse',)
        }),
        
        ('🔭 Telescope Pointing', {
            'fields': (
                ('unit_ra', 'unit_dec'),
                ('unit_alt', 'unit_az'),
                'airmass'
            ),
            'description': 'Actual telescope coordinates',
            'classes': ('collapse',)
        }),
        
        ('🌙 Moon & Strategy (TCSpy)', {
            'fields': (
                'moon_sep', 'moon_phase', 'obsmode', 'specmode', 'ntels'
            ),
            'description': 'Observation strategy information (TCSpy only)',
            'classes': ('collapse',)
        }),
        
        ('🎚️ Focus & Guiding', {
            'fields': (
                ('focuser_position', 'af_time'),
                ('af_value', 'af_error'),
                ('guiding_enabled', 'guiding_rms_total'),
                ('guiding_rms_ra', 'guiding_rms_dec')
            ),
            'description': 'Focus and guiding information',
            'classes': ('collapse',)
        }),
        
        ('🌦️ Weather Conditions', {
            'fields': (
                'weather_update_time',
                ('ambient_temperature', 'humidity'),
                ('pressure', 'dew_point'),
                ('wind_speed', 'wind_direction', 'wind_gust'),
                ('sky_brightness', 'sky_temperature'),
                ('cloud_fraction', 'rain_rate')
            ),
            'description': 'Environmental conditions',
            'classes': ('collapse',)
        }),
        
        ('⭐ Image Quality', {
            'fields': (
                'fwhm', 'background_level', 'num_sources',
                'star_count', 'median_hfd', 'nina_hfr',
                'limiting_magnitude', 'ellipticity'
            ),
            'description': 'Image quality metrics',
            'classes': ('collapse',)
        }),
        
        ('🎲 Plate Solving (NINA)', {
            'fields': (
                'plate_solved',
                ('plate_solve_ra', 'plate_solve_dec'),
                ('plate_solve_angle', 'plate_solve_pixel_scale')
            ),
            'description': 'Plate solving results (NINA only)',
            'classes': ('collapse',)
        }),
        
        ('📝 Observation Notes', {
            'fields': (
                'obsnote', 'sequence_title', 'sequence_target',
                ('is_too')
            ),
            'description': 'Notes and flags',
            'classes': ('collapse',)
        }),
        
        ('🔧 Technical Details', {
            'fields': (
                'software_used', 'software_version', 'observer',
                'instrument', ('ccdtemp', 'set_ccdtemp', 'cooler_power'),
                ('pixscale_x', 'pixscale_y'), 'image_id',
                ('jd', 'mjd'), 'header_parsed'
            ),
            'description': 'Technical and header information',
            'classes': ('collapse',)
        }),
        
        ('📊 Metadata', {
            'fields': (
                'processing_status', 'fits_header_cache',
                'created_at', 'updated_at'
            ),
            'classes': ('collapse',)
        }),
    )
    
    # === Helper method for data completeness ===
    def _calculate_data_completeness(self, frame):
        """Calculate data completeness percentage for a frame."""
        fields_to_check = [
            'object_ra', 'object_dec', 'airmass', 'ambient_temperature',
            'humidity', 'wind_speed', 'focuser_position', 'fwhm'
        ]
        
        populated_fields = sum(1 for field in fields_to_check if getattr(frame, field) is not None)
        return int((populated_fields / len(fields_to_check)) * 100)
    
    # === Enhanced list display with dynamic filtering ===
    def get_list_display(self, request):
        """Customize list display based on data content."""
        base_display = [
            'original_filename', 'unit', 'night', 'object_name', 
            'filter', 'exptime', 'obstime', 'software_used'
        ]
        
        # Add conditional columns based on available data
        if ScienceFrame.objects.filter(fwhm__isnull=False).exists():
            base_display.append('fwhm')
        
        if ScienceFrame.objects.filter(airmass__isnull=False).exists():
            base_display.append('airmass')
            
        return base_display
    
    # === Software-aware list filters ===
    def get_list_filter(self, request):
        """Customize list filters based on available data."""
        base_filters = [
            'unit', 'night', 'filter', 'software_used', 'header_parsed'
        ]
        
        # Add conditional filters
        if ScienceFrame.objects.filter(object_type__isnull=False).exclude(object_type='').exists():
            base_filters.append('object_type')
        
        if ScienceFrame.objects.filter(obsmode__isnull=False).exclude(obsmode='').exists():
            base_filters.append('obsmode')
            
        if ScienceFrame.objects.filter(is_too=True).exists():
            base_filters.append('is_too')
            
        return base_filters
    
    # === Color-coded display methods ===
    def software_used_display(self, obj):
        """Display software with color coding."""
        if obj.software_used == 'nina':
            return f'🔵 {obj.software_used.upper()}'
        elif obj.software_used == 'tcspy':
            return f'🟣 {obj.software_used.upper()}'
        else:
            return f'⚪ {obj.software_used.upper()}'
    software_used_display.short_description = 'Software'
    
    def fwhm_display(self, obj):
        """Display FWHM with quality indicator."""
        if obj.fwhm:
            if obj.fwhm <= 2.0:
                return f'✅ {obj.fwhm:.2f}"'
            elif obj.fwhm <= 3.0:
                return f'⚠️ {obj.fwhm:.2f}"'
            else:
                return f'❌ {obj.fwhm:.2f}"'
        return '—'
    fwhm_display.short_description = 'FWHM'
    
    def header_status(self, obj):
        """Display header parsing status."""
        if obj.header_parsed:
            return '✅ Parsed'
        else:
            return '❌ Not Parsed'
    header_status.short_description = 'Header'
    header_status.boolean = True
    
    # === Enhanced fieldsets for different software ===
    def get_fieldsets(self, request, obj=None):
        """Customize fieldsets based on software used."""
        fieldsets = list(self.fieldsets)
        
        if obj and obj.software_used == 'nina':
            # Customize for NINA data
            for i, (title, opts) in enumerate(fieldsets):
                if 'Moon & Strategy (TCSpy)' in title:
                    # Minimize TCSpy-only sections for NINA
                    opts['classes'] = opts.get('classes', ()) + ('collapse',)
                    opts['description'] = 'TCSpy-only fields (limited for NINA data)'
                elif 'Plate Solving (NINA)' in title:
                    # Expand NINA-specific sections
                    opts['classes'] = tuple(c for c in opts.get('classes', ()) if c != 'collapse')
        
        elif obj and obj.software_used == 'tcspy':
            # Customize for TCSpy data
            for i, (title, opts) in enumerate(fieldsets):
                if 'Plate Solving (NINA)' in title:
                    # Minimize NINA-only sections for TCSpy
                    opts['classes'] = opts.get('classes', ()) + ('collapse',)
                    opts['description'] = 'NINA-only fields (not available for TCSpy data)'
        
        return fieldsets
    
    # === Custom admin actions ===
    def reparse_headers(self, request, queryset):
        """Re-parse FITS headers for selected frames."""
        success_count = 0
        error_count = 0
        
        for frame in queryset:
            try:
                frame.parse_fits_header()
                frame.save()
                success_count += 1
            except Exception as e:
                error_count += 1
        
        if success_count:
            self.message_user(request, f'Successfully re-parsed headers for {success_count} frames.')
        if error_count:
            self.message_user(request, f'Failed to re-parse headers for {error_count} frames.', level='ERROR')
    
    reparse_headers.short_description = "Re-parse FITS headers"
    
    def mark_test_observations(self, request, queryset):
        """Mark selected frames as test observations."""
        updated = queryset.update(is_test_observation=True)
        self.message_user(request, f'Marked {updated} frames as test observations.')
    
    mark_test_observations.short_description = "Mark as test observations"
    
    actions = ['reparse_headers', 'mark_test_observations']
    
    # === Enhanced change view ===
    def change_view(self, request, object_id, form_url='', extra_context=None):
        """Enhanced change view with additional context."""
        extra_context = extra_context or {}
        
        if object_id:
            obj = self.get_object(request, object_id)
            if obj:
                # Add software-specific context
                extra_context.update({
                    'is_nina': obj.software_used == 'nina',
                    'is_tcspy': obj.software_used == 'tcspy',
                    'has_coordinates': bool(obj.object_ra and obj.object_dec),
                    'has_weather': bool(obj.ambient_temperature),
                    'has_focus': bool(obj.focuser_position),
                    'has_guiding': bool(obj.guiding_enabled),
                    'data_completeness': self._calculate_data_completeness(obj)
                })
        
        return super().change_view(request, object_id, form_url, extra_context)

    def unified_filename_display(self, obj):
        """Display unified filename with truncation."""
        if obj.unified_filename:
            if len(obj.unified_filename) > 35:
                return f"{obj.unified_filename[:35]}..."
            return obj.unified_filename
        return "—"
    unified_filename_display.short_description = 'Unified Filename'
    
    def obstime_display(self, obj):
        """Display observation time in readable format."""
        if obj.obstime:
            return obj.obstime.strftime('%Y-%m-%d %H:%M:%S')
        return "—"
    obstime_display.short_description = 'Obs Time'
    
    def mjd_display(self, obj):
        """Display MJD with appropriate precision."""
        if obj.mjd:
            return f"{obj.mjd:.6f}"
        return "—"
    mjd_display.short_description = 'MJD'

    list_per_page = 50
