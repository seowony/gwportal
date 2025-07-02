"""
7DT Telescope Facility Models
----------------------------

This module defines the database models for the 7DT telescope array facility.
Each telescope unit (7DT01, 7DT02, etc.) has exactly one mount, camera, 
filter wheel, and focuser attached. The models track equipment relationships
and record history when components are moved between units.

Configuration data is read from:
/home/7dt/7dt_too/backend/data/7dt/filtinfo.dict
/home/7dt/7dt_too/backend/data/7dt/multitelescopes.dict
"""

import uuid
from django.db import models
from django.core.exceptions import ValidationError
from django.core.validators import MinValueValidator, MaxValueValidator
from django.contrib.auth.models import User
from django.utils import timezone


class Unit(models.Model):
    """
    Represents a telescope unit (7DT01, 7DT02, etc.)
    """
    STATUS_CHOICES = [
        ('idle', 'Idle'),
        ('busy', 'Busy'),
        ('offline', 'Offline')
    ]

    name = models.CharField(max_length=5)     # 7DT01 to 7DT20
    model = models.CharField(max_length=20, default='DeltaRho 500')
    manufacturer = models.CharField(max_length=20, default='PlaneWave')
    fov = models.FloatField(default=2.62)  # Field of View in degrees
    f_ratio = models.FloatField(default=3.0)  # Focal ratio
    focal_length = models.FloatField(default=232.8)  # Focal length in mm
    diameter = models.FloatField(default=508.0)  # Diameter in mm
    weight = models.FloatField(default=75.0)  # Weight in kg
    status = models.CharField(max_length=10, choices=STATUS_CHOICES, default='idle')
    status_update_time = models.DateTimeField(null=True, blank=True)
    notes = models.TextField(blank=True)

    class Meta:
        ordering = ['name']
        indexes = [models.Index(fields=['name'])]
        
    def __str__(self):
        return self.name
    
    @property
    def is_active(self):
        """Returns True if the unit is not offline"""
        return self.status != 'offline'


class Mount(models.Model):
    """
    Represents a telescope mount
    """
    name = models.CharField(max_length=50, blank=True)  # From multitelescopes.dict
    unit = models.OneToOneField(Unit, on_delete=models.CASCADE, related_name='mount')
    device_type = models.CharField(max_length=20, default='PWI4')
    host_ip = models.CharField(max_length=15, default='10.0.106.6')
    port_num = models.CharField(max_length=10, default='8220')
    device_num = models.IntegerField(default=0)
    park_alt = models.FloatField(default=40.0)
    park_az = models.FloatField(default=300.0)
    
    def __str__(self):
        return f"Mount for {self.unit.name}"


class Filter(models.Model):
    """
    Represents a filter that can be placed in filter wheels
    """

    MEDIUMBAND_CHOICES = [
        (f'm{i}', f'm{i}') for i in range(350, 925, 25)
    ] + [
        (f'm{i}w', f'm{i}w') for i in range(350, 925, 25)
    ]

    UGRIZ_CHOICES = [
        ('u', 'Sloan u'), ('g', 'Sloan g'), ('r', 'Sloan r'),
        ('i', 'Sloan i'), ('z', 'Sloan z')
    ]

    FILTER_CHOICES = MEDIUMBAND_CHOICES + UGRIZ_CHOICES

    name = models.CharField(max_length=6, choices=FILTER_CHOICES)
    central_wl = models.FloatField(help_text="Central wavelength in nanometers")
    width = models.FloatField(help_text="Filter bandwidth in nanometers")
    manufacturer = models.CharField(max_length=50, blank=True, null=True)
    uid = models.UUIDField(default=uuid.uuid4, editable=False, unique=True)
    
    class Meta:
        ordering = ['central_wl']
    
    def __str__(self):
        return f"{self.name} ({self.central_wl}nm)"
    
    def save(self, *args, **kwargs):
        # Auto-calculate wavelength and width based on filter name
        if self.name.startswith('m'):
            if self.name.endswith('w'):
                self.central_wl = float(self.name[1:-1])
                self.width = 50.0
            else:
                self.central_wl = float(self.name[1:])
                self.width = 25.0
        elif self.name in ['u', 'g', 'r', 'i', 'z']:
            ugriz_defaults = {
                'u': {'central_wl': 354.3, 'width': 59.2},
                'g': {'central_wl': 477.0, 'width': 137.0},
                'r': {'central_wl': 623.1, 'width': 137.0},
                'i': {'central_wl': 762.5, 'width': 153.0},
                'z': {'central_wl': 913.4, 'width': 95.0}
            }
            self.central_wl = ugriz_defaults[self.name]['central_wl']
            self.width = ugriz_defaults[self.name]['width']
        
        super().save(*args, **kwargs)


class FilterWheel(models.Model):
    """
    Represents a filter wheel attached to a unit
    """
    name = models.CharField(max_length=50, blank=True)  # From multitelescopes.dict
    unit = models.OneToOneField(Unit, on_delete=models.CASCADE, related_name='filter_wheel',
                              null=True, blank=True)
    filters = models.ManyToManyField(Filter, through='FilterPosition')
    uid = models.CharField(max_length=20, unique=True)  # FW01 to FW20
    current_position = models.IntegerField(null=True, blank=True)
    
    def __str__(self):
        return f"{self.uid} ({self.unit.name})"
    
    def get_current_filter(self):
        if self.current_position:
            try:
                position = self.filterposition_set.get(position=self.current_position)
                return position.filter
            except FilterPosition.DoesNotExist:
                return None
        return None


class FilterPosition(models.Model):
    """
    Maps which filter is in which position of a filter wheel
    """
    filter_wheel = models.ForeignKey(FilterWheel, on_delete=models.CASCADE)
    filter = models.ForeignKey(Filter, on_delete=models.CASCADE)
    position = models.IntegerField(validators=[MinValueValidator(1), MaxValueValidator(9)])
    uid = models.UUIDField(default=uuid.uuid4, editable=False)
    
    class Meta:
        unique_together = [('filter_wheel', 'position')]
    
    def __str__(self):
        return f"{self.filter.name} at position {self.position} in {self.filter_wheel.uid}"


class FilterOffset(models.Model):
    """
    Stores focus offset values for each filter in a filter wheel.
    Based on filter.offset file data.
    """
    filter = models.ForeignKey(Filter, on_delete=models.CASCADE)
    filter_wheel = models.ForeignKey(FilterWheel, on_delete=models.CASCADE)
    offset = models.IntegerField(default=999)
    error = models.IntegerField(default=999)
    updated_date = models.DateTimeField(auto_now=True)
    
    class Meta:
        unique_together = ['filter', 'filter_wheel']
    
    def __str__(self):
        return f"Offset for {self.filter.name} on {self.filter_wheel}"


class Focuser(models.Model):
    """
    Represents a telescope focuser
    """
    name = models.CharField(max_length=50, blank=True)  # From multitelescopes.dict
    unit = models.OneToOneField(Unit, on_delete=models.CASCADE, related_name='focuser')
    device_type = models.CharField(max_length=50, default='PWI4')
    host_ip = models.CharField(max_length=15, default='10.0.106.6')
    port_num = models.CharField(max_length=10, default='8220')
    device_num = models.IntegerField(default=0)
    min_step = models.IntegerField(default=2000)
    max_step = models.IntegerField(default=14000)
    check_time = models.FloatField(default=0.5)

    def __str__(self):
        return f"Focuser for {self.unit.name}"


class Camera(models.Model):
    """
    Represents a camera attached to a unit
    """
    name = models.CharField(max_length=20)  # CAM01 to CAM20
    serial_number = models.CharField(max_length=20, unique=True)
    byname = models.CharField(max_length=20, null=True, blank=True)  # e.g. 'Athens', 'Baltimore'
    
    # Relationships
    unit = models.OneToOneField(Unit, on_delete=models.CASCADE, related_name='camera', 
                               null=True, blank=True)  # Make nullable for migration
    filter_wheel = models.OneToOneField(FilterWheel, on_delete=models.SET_NULL, 
                               null=True, blank=True, related_name='camera')
    
    # Rest of the fields remain the same
    manufacturer = models.CharField(max_length=20, default='Moravian')
    model = models.CharField(max_length=20, default='C3-61000 Pro')
    pixel_size = models.FloatField(default=3.76)
    dimension_x = models.IntegerField(default=9576)
    dimension_y = models.IntegerField(default=6388)
    sensor_name = models.CharField(max_length=25, default='Sony IMX455 CMOS')
    rdnoise = models.FloatField(default=3.5)
    gain = models.FloatField(default=1.0)
    notes = models.TextField(blank=True)
    
    class Meta:
        ordering = ['name']
    
    def __str__(self):
        return f"Camera {self.name} ({self.byname})" if self.byname else f"Camera {self.name}"

# History tracking models

class CameraHistory(models.Model):
    """
    Tracks the history of camera movements between units
    """
    camera = models.ForeignKey(Camera, on_delete=models.CASCADE)
    from_unit = models.ForeignKey(Unit, on_delete=models.SET_NULL, null=True, related_name='camera_history_from')
    to_unit = models.ForeignKey(Unit, on_delete=models.SET_NULL, null=True, related_name='camera_history_to')
    timestamp = models.DateTimeField(auto_now_add=True)
    reason = models.TextField(blank=True, help_text="Reason for this change")
    
    class Meta:
        ordering = ['-timestamp']
        verbose_name_plural = "Camera Histories"
    
    def __str__(self):
        return f"{self.camera} moved from {self.from_unit} to {self.to_unit} on {self.timestamp}"


class FilterWheelHistory(models.Model):
    """
    Tracks the history of filter wheel movements between units
    """
    filter_wheel = models.ForeignKey(FilterWheel, on_delete=models.CASCADE)
    from_unit = models.ForeignKey(Unit, on_delete=models.SET_NULL, null=True, related_name='filterwheel_history_from')
    to_unit = models.ForeignKey(Unit, on_delete=models.SET_NULL, null=True, related_name='filterwheel_history_to')
    timestamp = models.DateTimeField(auto_now_add=True)
    reason = models.TextField(blank=True, help_text="Reason for this change")
    
    class Meta:
        ordering = ['-timestamp']
        verbose_name_plural = "Filter Wheel Histories"
    
    def __str__(self):
        return f"{self.filter_wheel} moved from {self.from_unit} to {self.to_unit} on {self.timestamp}"


class FilterHistory(models.Model):
    """
    Tracks the history of filter movements between units
    """
    filter = models.ForeignKey(Filter, on_delete=models.CASCADE)
    from_unit = models.ForeignKey(Unit, on_delete=models.SET_NULL, null=True, related_name='filter_history_from')
    to_unit = models.ForeignKey(Unit, on_delete=models.SET_NULL, null=True, related_name='filter_history_to')
    timestamp = models.DateTimeField(auto_now_add=True)
    reason = models.TextField(blank=True, help_text="Reason for this change")
    
    class Meta:
        ordering = ['-timestamp']
        verbose_name_plural = "Filter Histories"
    
    def __str__(self):
        return f"{self.filter} moved from {self.from_unit} to {self.to_unit} on {self.timestamp}"

# Add to facility/models.py
class Weather(models.Model):
    """
    Stores weather information from the observatory site
    Updated approximately every minute
    """
    timestamp = models.DateTimeField(primary_key=True)
    temperature = models.FloatField(null=True, blank=True, help_text="Temperature in Celsius")
    humidity = models.FloatField(null=True, blank=True, help_text="Relative humidity in percentage")
    pressure = models.FloatField(null=True, blank=True, help_text="Atmospheric pressure in hPa")
    wind_speed = models.FloatField(null=True, blank=True, help_text="Wind speed in m/s")
    wind_direction = models.FloatField(null=True, blank=True, help_text="Wind direction in degrees")
    cloud_cover = models.FloatField(null=True, blank=True, help_text="Cloud coverage percentage")
    seeing = models.FloatField(null=True, blank=True, help_text="Astronomical seeing in arcseconds")
    rain_rate = models.FloatField(null=True, blank=True, help_text="Rain rate in mm/hour")
    sky_temperature = models.FloatField(null=True, blank=True, help_text="Sky temperature in Celsius")
    dew_point = models.FloatField(null=True, blank=True, help_text="Dew point in Celsius")
    ambient_light = models.FloatField(null=True, blank=True, help_text="Ambient light level in lux")
    safe_status = models.BooleanField(default=False, help_text="Indicates if weather conditions are safe for observing")
    
    class Meta:
        ordering = ['-timestamp']
        verbose_name_plural = 'Weather'
        
    def __str__(self):
        return f"Weather at {self.timestamp}"
