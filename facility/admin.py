from django.contrib import admin
from django.utils.html import format_html
from django.urls import reverse

from .models import (
    Unit, Filter, FilterWheel, FilterPosition, Camera, Mount, Focuser,
    FilterOffset, CameraHistory, FilterWheelHistory, FilterHistory, Weather
)

# Inline admin classes for related models
class CameraInline(admin.TabularInline):
    model = Camera
    extra = 0
    fields = ['name', 'byname', 'serial_number', 'filter_wheel']

class FilterWheelInline(admin.TabularInline):
    model = FilterWheel
    extra = 0
    fields = ['uid', 'name']

class MountInline(admin.TabularInline):
    model = Mount
    extra = 0

class FocuserInline(admin.TabularInline):
    model = Focuser
    extra = 0
    
class FilterPositionInline(admin.TabularInline):
    model = FilterPosition
    extra = 0
    autocomplete_fields = ['filter']

@admin.register(Unit)
class UnitAdmin(admin.ModelAdmin):
    search_fields = ['name']
    list_display = ['name', 'status', 'status_update_time', 'get_camera', 'get_filter_wheel']
    list_filter = ['status']
    inlines = [CameraInline, FilterWheelInline, MountInline, FocuserInline]
    
    def get_camera(self, obj):
        try:
            camera = Camera.objects.get(unit=obj)
            return format_html('<a href="{}">{}</a>',
                reverse('admin:facility_camera_change', args=[camera.id]),
                camera.name
            )
        except Camera.DoesNotExist:
            return "—"
    get_camera.short_description = "Camera"
    
    def get_filter_wheel(self, obj):
        try:
            fw = FilterWheel.objects.get(unit=obj)
            return format_html('<a href="{}">{}</a>',
                reverse('admin:facility_filterwheel_change', args=[fw.id]),
                fw.uid
            )
        except FilterWheel.DoesNotExist:
            return "—"
    get_filter_wheel.short_description = "Filter Wheel"

@admin.register(Camera)
class CameraAdmin(admin.ModelAdmin):
    search_fields = ['name', 'byname', 'serial_number']
    list_display = ['name', 'byname', 'serial_number', 'unit_link', 'filter_wheel_link']
    list_filter = ['model', 'manufacturer']
    autocomplete_fields = ['unit', 'filter_wheel']
    
    fieldsets = (
        ('Identification', {
            'fields': ('name', 'byname', 'serial_number')
        }),
        ('Relationships', {
            'fields': ('unit', 'filter_wheel')
        }),
        ('Camera Details', {
            'fields': ('manufacturer', 'model', 'pixel_size', 'rdnoise', 'gain', 'dimension_x', 'dimension_y', 'sensor_name')
        }),
    )
    
    def unit_link(self, obj):
        if obj.unit:
            return format_html('<a href="{}">{}</a>',
                reverse('admin:facility_unit_change', args=[obj.unit.id]),
                obj.unit.name
            )
        return "—"
    unit_link.short_description = "Unit"
    
    def filter_wheel_link(self, obj):
        if obj.filter_wheel:
            return format_html('<a href="{}">{}</a>',
                reverse('admin:facility_filterwheel_change', args=[obj.filter_wheel.id]),
                obj.filter_wheel.uid
            )
        return "—"
    filter_wheel_link.short_description = "Filter Wheel"

@admin.register(Filter)
class FilterAdmin(admin.ModelAdmin):
    search_fields = ['name', 'uid']
    list_display = ['name', 'central_wl', 'width', 'uid', 'get_positions']
    list_filter = ['manufacturer']
    
    def get_positions(self, obj):
        positions = FilterPosition.objects.filter(filter=obj)
        if positions:
            return format_html(', '.join([
                f'<a href="{reverse("admin:facility_filterwheel_change", args=[p.filter_wheel.id])}">{p.filter_wheel.uid}:{p.position}</a>'
                for p in positions
            ]))
        return "—"
    get_positions.short_description = "Positions"

@admin.register(FilterWheel)
class FilterWheelAdmin(admin.ModelAdmin):
    search_fields = ['uid', 'name']
    list_display = ['uid', 'name', 'unit_link', 'get_filter_count']
    inlines = [FilterPositionInline]
    
    def unit_link(self, obj):
        if obj.unit:
            return format_html('<a href="{}">{}</a>',
                reverse('admin:facility_unit_change', args=[obj.unit.id]),
                obj.unit.name
            )
        return "—"
    unit_link.short_description = "Unit"
    
    def get_filter_count(self, obj):
        count = FilterPosition.objects.filter(filter_wheel=obj).count()
        return count
    get_filter_count.short_description = "Filters"

@admin.register(FilterPosition)
class FilterPositionAdmin(admin.ModelAdmin):
    search_fields = ['filter_wheel__uid', 'filter__name']
    list_display = ['id', 'filter_wheel', 'filter', 'position']
    list_filter = ['filter_wheel']
    autocomplete_fields = ['filter_wheel', 'filter']

@admin.register(Mount)
class MountAdmin(admin.ModelAdmin):
    search_fields = ['name']
    list_display = ['name', 'unit', 'device_type', 'park_alt', 'park_az']
    list_filter = ['device_type']

@admin.register(Focuser)
class FocuserAdmin(admin.ModelAdmin):
    search_fields = ['name']
    list_display = ['name', 'unit', 'device_type', 'min_step', 'max_step']
    list_filter = ['device_type']

@admin.register(FilterOffset)
class FilterOffsetAdmin(admin.ModelAdmin):
    search_fields = ['filter__name']
    list_display = ['filter', 'offset']
    list_filter = ['filter']
    autocomplete_fields = ['filter']

@admin.register(CameraHistory)
class CameraHistoryAdmin(admin.ModelAdmin):
    search_fields = ['camera__name', 'camera__serial_number']
    list_display = ['camera', 'from_unit', 'to_unit', 'reason', 'timestamp']
    list_filter = ['camera']
    date_hierarchy = 'timestamp'
    readonly_fields = ['timestamp']
    autocomplete_fields = ['camera', 'from_unit', 'to_unit']

@admin.register(FilterWheelHistory)
class FilterWheelHistoryAdmin(admin.ModelAdmin):
    search_fields = ['filter_wheel__uid']
    list_display = ['filter_wheel', 'from_unit', 'to_unit', 'reason', 'timestamp']
    list_filter = ['filter_wheel']
    date_hierarchy = 'timestamp'
    readonly_fields = ['timestamp']
    autocomplete_fields = ['filter_wheel', 'from_unit', 'to_unit']

@admin.register(FilterHistory)
class FilterHistoryAdmin(admin.ModelAdmin):
    search_fields = ['filter__name']
    list_display = ['filter', 'from_unit', 'to_unit', 'reason', 'timestamp']
    list_filter = ['filter']
    date_hierarchy = 'timestamp'
    readonly_fields = ['timestamp']
    autocomplete_fields = ['filter', 'from_unit', 'to_unit']

if hasattr(Weather, 'timestamp'):  # Only register if Weather model exists and has timestamp field
    @admin.register(Weather)
    class WeatherAdmin(admin.ModelAdmin):
        list_display = ['timestamp', 'temperature', 'humidity', 'pressure', 'wind_speed', 'wind_direction', 'cloud_cover', 'seeing', 'rain_rate', 'sky_temperature', 'dew_point', 'ambient_light', 'safe_status']
        list_filter = ['safe_status']
        date_hierarchy = 'timestamp'
        readonly_fields = ['timestamp']
