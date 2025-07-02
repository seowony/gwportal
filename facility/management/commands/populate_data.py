import json
import os
import time
import uuid
import traceback
from django.core.management.base import BaseCommand
from django.utils import timezone
from django.utils.timezone import make_aware
from django.db import IntegrityError, transaction
from facility.models import (
    Unit, Filter, FilterWheel, FilterPosition, Camera, Mount, Focuser,
    FilterOffset, CameraHistory, FilterHistory, FilterWheelHistory, Weather
)

class Command(BaseCommand):
    help = 'Populate telescope unit data from configuration files'

    def add_arguments(self, parser):
        parser.add_argument(
            '--config-path',
            default='/home/7dt/7dt_too/backend/data/7dt',
            help='Path to configuration directory'
        )
        parser.add_argument(
            '--once',
            action='store_true',
            help='Run once instead of continuous monitoring'
        )
        parser.add_argument(
            '--interval',
            type=int,
            default=60,
            help='Check interval in seconds (for monitoring mode)'
        )
        # Add data flush option
        parser.add_argument(
            '--flush',
            action='store_true',
            help='Delete all existing data before populating'
        )
        # Add flush-only option
        parser.add_argument(
            '--flush-only',
            action='store_true',
            help='Only delete all existing data without populating'
        )
        parser.add_argument(
            '--with-weather',
            action='store_true',
            help='Also process weather data'
        )

    def handle(self, *args, **options):
        config_path = options['config_path']
        run_once = options['once']
        interval = options['interval']
        flush = options['flush']
        flush_only = options['flush_only']  # New flush-only option

        # If flush-only is provided, just perform the database flush and exit
        if flush_only:
            self.stdout.write(self.style.WARNING('Performing database flush only...'))
            self._flush_data()
            self.stdout.write(self.style.SUCCESS('Database flush completed! No data was populated.'))
            return

        # Regular processing with optional flush before populating
        filtinfo_path = os.path.join(config_path, 'filtinfo.dict')
        multitelescopes_path = os.path.join(config_path, 'multitelescopes.dict')
        
        if not os.path.exists(filtinfo_path) or not os.path.exists(multitelescopes_path):
            self.stdout.write(self.style.ERROR(f"Configuration files not found in {config_path}"))
            return

        # Execute data flush if requested
        if flush:
            self._flush_data()
        
        last_filt_modified = None
        last_multi_modified = None
        
        try:
            if run_once:
                self.stdout.write(self.style.SUCCESS('Starting one-time configuration update...'))
                self._process_configurations(filtinfo_path, multitelescopes_path)

                # Process weather data if requested
                if options['with_weather']:
                    weather_path = os.path.join(config_path, 'weatherinfo.dict')
                    self._process_weather(weather_path)

                self.stdout.write(self.style.SUCCESS('Configuration update complete!'))
                return
                
            # Continuous monitoring
            self.stdout.write(self.style.SUCCESS(f'Monitoring configuration files in: {config_path}'))
            self.stdout.write(self.style.SUCCESS(f'Check interval: {interval} seconds'))

            last_filt_modified = None
            last_multi_modified = None
            last_weather_modified = None
            weather_path = os.path.join(config_path, 'weatherinfo.dict')

            while True:
                # Check for changes in filtinfo.dict
                try:
                    current_filt_modified = os.path.getmtime(filtinfo_path)
                    if last_filt_modified is None or current_filt_modified > last_filt_modified:
                        self.stdout.write(self.style.SUCCESS('Detected change in filtinfo.dict'))
                        self._process_configurations(filtinfo_path, multitelescopes_path)
                        last_filt_modified = current_filt_modified
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"Error checking filtinfo.dict: {e}"))
                
                # Check for changes in multitelescopes.dict
                try:
                    current_multi_modified = os.path.getmtime(multitelescopes_path)
                    if last_multi_modified is None or current_multi_modified > last_multi_modified:
                        self.stdout.write(self.style.SUCCESS('Detected change in multitelescopes.dict'))
                        self._process_configurations(filtinfo_path, multitelescopes_path)
                        last_multi_modified = current_multi_modified
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"Error checking multitelescopes.dict: {e}"))

                # Check for changes in weatherinfo.dict if requested
                if options['with_weather']:
                    try:
                        current_weather_modified = os.path.getmtime(weather_path)
                        if last_weather_modified is None or current_weather_modified > last_weather_modified:
                            self.stdout.write(self.style.SUCCESS('Detected change in weatherinfo.dict'))
                            self._process_weather(weather_path)
                            last_weather_modified = current_weather_modified
                    except Exception as e:
                        self.stdout.write(self.style.ERROR(f"Error checking weatherinfo.dict: {e}"))

                time.sleep(interval)
                
        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING('Stopping the monitoring process'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error: {e}'))
            self.stdout.write(traceback.format_exc())

    def _flush_data(self):
        """Delete all telescope configuration data from the database"""
        self.stdout.write(self.style.WARNING('Flushing all telescope configuration data...'))
        
        # Use transaction for safe data deletion
        with transaction.atomic():
            # Delete relationships first
            CameraHistory.objects.all().delete()
            FilterWheelHistory.objects.all().delete()
            FilterHistory.objects.all().delete()
            FilterPosition.objects.all().delete()
            FilterOffset.objects.all().delete()
            
            # Then delete equipment records
            Camera.objects.all().delete()
            FilterWheel.objects.all().delete()
            Focuser.objects.all().delete()
            Mount.objects.all().delete()
            Filter.objects.all().delete()
            Unit.objects.all().delete()
            
        self.stdout.write(self.style.SUCCESS('Database cleared successfully!'))

    def _process_configurations(self, filtinfo_path, multitelescopes_path):
        """Process both configuration files and update database"""
        try:
            # Load configuration files
            with open(filtinfo_path, 'r') as f:
                filter_sets = json.load(f)
            
            with open(multitelescopes_path, 'r') as f:
                telescope_status = json.load(f)
            
            self._update_telescopes(filter_sets, telescope_status)
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error processing configurations: {e}"))
            self.stdout.write(traceback.format_exc())

    def _update_telescopes(self, filter_sets, telescope_status):
        """Update telescope units and their components based on configuration data"""
        # List of bynames for cameras
        bynames = [
            'Athens', 'Baltimore', 'Cambridge', 'Denver', 'Edinburgh', 'Flagstaff',
            'Greenwich', 'Heidelberg', 'Istanbul', 'Jodrell Bank', 'Kitt Peak', 'La Palma',
            'Mauna Kea', 'New Haven', 'Oxford', 'Pasadena', 'Quanzhou', 'Rome',
            'Siding Spring', 'Tokyo'
        ]

        # Process each telescope unit
        for unit_name, unit_status in telescope_status.items():
            # Skip non-telescope entries
            if not unit_name.startswith('7DT'):
                continue
            
            # Extract unit number (e.g., '7DT01' -> 1)
            try:
                unit_num = int(unit_name[3:])
            except ValueError:
                self.stdout.write(self.style.WARNING(f"Invalid unit name format: {unit_name}"))
                continue
            
            # Get filter configuration for this unit
            filters = filter_sets.get(unit_name, [])
            
            # Create or update Unit
            unit, created = Unit.objects.get_or_create(
                name=unit_name,
                defaults={
                    'model': 'DeltaRho 500',
                    'manufacturer': 'PlaneWave',
                    'status': unit_status.get('Status', 'idle').lower(),
                    'fov': 2.62,
                    'f_ratio': 3.0,
                    'focal_length': 232.8,
                    'diameter': 508.0,
                    'weight': 75.0
                }
            )
            
            if created:
                self.stdout.write(self.style.SUCCESS(f'Created Unit: {unit_name}'))
            else:
                # Update status from multitelescopes.dict
                unit.status = unit_status.get('Status', 'idle').lower()
                if 'Status_update_time' in unit_status:
                    try:
                        # Convert to timezone-aware datetime
                        naive_datetime = timezone.datetime.fromisoformat(unit_status['Status_update_time'])
                        unit.status_update_time = make_aware(naive_datetime)
                    except (ValueError, TypeError):
                        unit.status_update_time = timezone.now()
                unit.save()
            
            # 1. Mount
            if 'Mount' in unit_status:
                mount_data = unit_status['Mount']

                # Create base_defaults dictionary with only fields that definitely exist
                base_defaults = {
                    'name': mount_data.get('name', ''),
                    'device_type': 'PWI4',
                    'host_ip': '10.0.106.6',
                    'port_num': '8220',
                    'device_num': 0,
                    'park_alt': 40.0,
                    'park_az': 300.0,
                }

                # Only add status fields if they actually exist in the model
                if hasattr(Mount, 'is_active'):
                    base_defaults['is_active'] = mount_data.get('is_active', True)
                if hasattr(Mount, 'status'):
                    base_defaults['status'] = mount_data.get('status', 'operational')
                                        
                # Create or get the mount with safe defaults
                mount, mount_created = Mount.objects.get_or_create(
                     unit=unit,
                     defaults=base_defaults
                )
                                                    
                if not mount_created:
                    changes = []
                    if mount.name != mount_data.get('name',''):
                        mount.name = mount_data.get('name', '')
                        changes.append(f"name: {mount.name}")
                        
                    # Update status fields if they exist in the model
                    if hasattr(mount, 'is_active') and mount.is_active != mount_data.get('is_active', True):
                        mount.is_active = mount_data.get('is_active', True)
                        changes.append(f"is_active: {mount.is_active}")

                    if hasattr(mount, 'status') and mount.status != mount_data.get('status', 'operational'):
                        mount.status = mount_data.get('status', 'operational')
                        changes.append(f"status: {mount.status}")

                    if changes:
                        mount.save()
                        self.stdout.write(self.style.SUCCESS(f'Updated Mount: {mount.name} for {unit_name}'))
            
            # 2. Focuser
            if 'Focuser' in unit_status:
                focuser_data = unit_status['Focuser']

                # Create base dictionary with only fields that definitely exist
                base_defaults = {
                    'name': focuser_data.get('name', ''),
                    'device_type': 'PWI4',
                    'host_ip': '10.0.106.6',
                    'port_num': '8220',
                    'device_num': 0,
                    'min_step': 2000,
                    'max_step': 14000,
                }

                # Only add optional fields if they exist in the model
                if hasattr(Focuser, 'is_active'):
                    base_defaults['is_active'] = focuser_data.get('is_active', True)
                if hasattr(Focuser, 'status'):
                    base_defaults['status'] = focuser_data.get('status', 'operational')

                focuser, focuser_created = Focuser.objects.get_or_create(
                    unit=unit,
                    defaults=base_defaults
                )

                if not focuser_created:
                    changes = []
                    # Update focuser properties
                    if focuser.name != focuser_data.get('name', ''):
                        focuser.name = focuser_data.get('name', '')
                        changes.append(f"name: {focuser.name}")

                    # Update status fields if they exist in the model
                    if hasattr(focuser, 'is_active') and focuser.is_active != focuser_data.get('is_active', True):
                        focuser.is_active = focuser_data.get('is_active', True)
                        changes.append(f"is_active: {focuser.is_active}")

                    if hasattr(focuser, 'status') and focuser.status != focuser_data.get('status', 'operational'):
                        focuser.status = focuser_data.get('status', 'operational')
                        changes.append(f"status: {focuser.status}")

                    if changes:
                        focuser.save()
                        self.stdout.write(self.style.SUCCESS(f'Updated Focuser: {focuser.name} for {unit_name}'))
            
            # 3. FilterWheel - Use get() first approach to avoid duplicates
            filter_wheel_name = f'FW{unit_num:02d}'
            filter_wheel_serial = ''
            filter_wheel_status = 'operational'
            filter_wheel_active = True
            
            if 'Filterwheel' in unit_status:
                fw_data = unit_status['Filterwheel']
                filter_wheel_serial = fw_data.get('name', '')
                if hasattr(FilterWheel, 'status'):
                    filter_wheel_status = fw_data.get('status', 'operational')
                if hasattr(FilterWheel, 'is_active'):
                    filter_wheel_active = fw_data.get('is_active', True)
            
            try:
                # First try to get by UID
                filter_wheel = FilterWheel.objects.get(uid=filter_wheel_name)
                fw_created = False
                
                changes = []
                if filter_wheel.name != filter_wheel_serial:
                    filter_wheel.name = filter_wheel_serial
                    changes.append(f"name: {filter_wheel_serial}")
    
                # Status fields if they exist
                if hasattr(filter_wheel, 'status') and filter_wheel.status != filter_wheel_status:
                    filter_wheel.status = filter_wheel_status
                    changes.append(f"status: {filter_wheel_status}")
    
                if hasattr(filter_wheel, 'is_active') and filter_wheel.is_active != filter_wheel_active:
                    filter_wheel.is_active = filter_wheel_active
                    changes.append(f"is_active: {filter_wheel_active}")
    
                # Check if filter wheel has moved to a different unit
                if filter_wheel.unit != unit:
                    FilterWheelHistory.objects.create(
                        filter_wheel=filter_wheel,
                        from_unit=filter_wheel.unit,
                        to_unit=unit,
                        reason="Updated from multitelescopes.dict"
                    )
                    filter_wheel.unit = unit
                    changes.append(f"unit: {unit.name}")

                if changes:
                    filter_wheel.save()
                    self.stdout.write(self.style.SUCCESS(f'Updated FilterWheel: {filter_wheel_name} ({filter_wheel_serial})'))
            
            except FilterWheel.DoesNotExist:
                # If it doesn't exist, create it
                create_kwargs = {
                    'uid': filter_wheel_name,
                    'unit': unit,
                    'name': filter_wheel_serial,
                }
                
                # Add optional fields if they exist in the model
                if hasattr(FilterWheel, 'status'):
                    create_kwargs['status'] = filter_wheel_status
                if hasattr(FilterWheel, 'is_active'):
                    create_kwargs['is_active'] = filter_wheel_active
                
                filter_wheel = FilterWheel.objects.create(**create_kwargs)
                fw_created = True
                self.stdout.write(self.style.SUCCESS(f'Created FilterWheel: {filter_wheel_name} ({filter_wheel_serial})'))
            
            # 4. Process filters
            self._process_filters(filter_wheel, filters)
            
            # 5. Camera - FIXED to handle serial number conflicts properly
            camera_name = f'CAM{unit_num:02d}'
            camera_byname = bynames[unit_num-1] if unit_num <= len(bynames) else None
            
            # Camera status properties
            camera_status = 'operational'
            camera_active = True
            
            # Get camera serial number - IMPROVED LOGIC FOR EMPTY NAMES
            camera_serial = None
            if 'Camera' in unit_status:
                cam_data = unit_status['Camera']
                camera_serial = cam_data.get('name', '')
                # Add status fields if they exist in the model
                if hasattr(Camera, 'status'):
                    camera_status = cam_data.get('status', 'operational')
                if hasattr(Camera, 'is_active'):
                    camera_active = cam_data.get('is_active', True)
            
            # Always create unique IDs for empty camera names
            if not camera_serial:
                # THIS IS THE FIX: Always use a unique identifier for empty names
                # Don't use fallback which causes conflicts
                camera_serial = f"{camera_name}-{uuid.uuid4().hex[:8]}"
            
            try:
                # First try to get camera by name
                camera = Camera.objects.get(name=camera_name)
                cam_created = False
                
                # Update status and active state if they exist in the model
                if hasattr(camera, 'is_active'):
                    camera.is_active = camera_active
                if hasattr(camera, 'status'):
                    camera.status = camera_status
                
                # Handle potential serial number updates safely
                if camera.serial_number != camera_serial:
                    try:
                        # Check if another camera already uses this serial
                        existing = Camera.objects.filter(serial_number=camera_serial).exclude(name=camera_name).first()
                        if existing:
                            self.stdout.write(self.style.WARNING(
                                f'Cannot update {camera_name} serial to {camera_serial} - already used by {existing.name}'
                            ))
                        else:
                            camera.serial_number = camera_serial
                    except Exception as e:
                        self.stdout.write(self.style.ERROR(f"Error updating camera serial: {e}"))
                
            except Camera.DoesNotExist:
                # Camera doesn't exist yet, create it
                try:
                    create_kwargs = {
                        'name': camera_name,
                        'serial_number': camera_serial,
                        'byname': camera_byname,
                        'unit': unit,
                        'filter_wheel': filter_wheel,
                        'manufacturer': 'Moravian',
                        'model': 'C3-61000 Pro',
                        'pixel_size': 3.76,
                        'rdnoise': 3.5,
                        'gain': 1.0,
                        'dimension_x': 9576,
                        'dimension_y': 6388,
                        'sensor_name': 'Sony IMX455 CMOS',
                    }
                    
                    # Add optional fields if they exist in the model
                    if hasattr(Camera, 'is_active'):
                        create_kwargs['is_active'] = camera_active
                    if hasattr(Camera, 'status'):
                        create_kwargs['status'] = camera_status
                    
                    camera = Camera.objects.create(**create_kwargs)
                    cam_created = True
                    self.stdout.write(self.style.SUCCESS(f'Created Camera: {camera_name} ({camera_serial})'))
                except IntegrityError:
                    # Handle duplicate serial number
                    unique_serial = f"{camera_serial}-{camera_name}"
                    self.stdout.write(self.style.WARNING(
                        f'Serial number conflict for {camera_serial}. Using {unique_serial}'
                    ))
                    
                    create_kwargs['serial_number'] = unique_serial
                    camera = Camera.objects.create(**create_kwargs)
                    cam_created = True
            
            # Update camera relationships if needed
            if not cam_created:
                changes = []

                # Check if camera has moved to a different unit
                if camera.unit != unit:
                    CameraHistory.objects.create(
                        camera=camera,
                        from_unit=camera.unit,
                        to_unit=unit,
                        reason="Updated from multitelescopes.dict"
                    )
                    camera.unit = unit
                    changes.append(f"unit: {unit.name}")

                # Check for other changes
                if camera.filter_wheel != filter_wheel:
                    camera.filter_wheel = filter_wheel
                    changes.append(f"filter_wheel: {filter_wheel.uid}")

                if camera.byname != camera_byname:
                    camera.byname = camera_byname
                    changes.append(f"byname: {camera_byname}")

                # Status fields if they exist
                if hasattr(camera, 'is_active') and camera.is_active != camera_active:
                    camera.is_active = camera_active
                    changes.append(f"is_active: {camera_active}")

                if hasattr(camera, 'status') and camera.status != camera_status:
                    camera.status = camera_status
                    changes.append(f"status: {camera_status}")

                if changes:
                    camera.save()
                    self.stdout.write(self.style.SUCCESS(f'Updated Camera: {camera_name} ({camera.serial_number})'))
        
        self.stdout.write(self.style.SUCCESS('Telescope configuration update complete.'))

    def _process_filters(self, filter_wheel, filters):
        """Process filters for a filter wheel"""
        # Define default values for filters
        ugriz_defaults = {
            'u': {'central_wl': 354.3, 'width': 59.2},
            'g': {'central_wl': 477.0, 'width': 137.0},
            'r': {'central_wl': 623.1, 'width': 137.0},
            'i': {'central_wl': 762.5, 'width': 153.0},
            'z': {'central_wl': 913.4, 'width': 95.0}
        }
        
        # Clear existing filter positions that are no longer in the config
        existing_filters = set(fp.filter.name for fp in FilterPosition.objects.filter(filter_wheel=filter_wheel))
        config_filters = set(f for f in filters if not f.startswith('Slot'))
        
        # Remove filters that are no longer in the configuration
        for position in FilterPosition.objects.filter(
            filter_wheel=filter_wheel, 
            filter__name__in=existing_filters - config_filters
        ):
            self.stdout.write(self.style.WARNING(
                f'Removing filter {position.filter.name} from position {position.position} in {filter_wheel.uid}'
            ))
            position.delete()
        
        # Add or update filters in FilterWheel
        for position, filter_name in enumerate(filters, start=1):
            if filter_name.startswith('Slot'):
                continue  # Skip slots
                
            # Determine filter specs based on name
            if filter_name in ugriz_defaults:
                central_wl = ugriz_defaults[filter_name]['central_wl']
                width = ugriz_defaults[filter_name]['width']
            elif filter_name.startswith('m'):
                if filter_name.endswith('w'):
                    central_wl = float(filter_name[1:-1])
                    width = 50.0
                else:
                    central_wl = float(filter_name[1:])
                    width = 25.0
            else:
                self.stdout.write(self.style.ERROR(f'Unknown filter type: {filter_name}'))
                continue
                
            # Create or get the filter
            filter_obj, filter_created = Filter.objects.get_or_create(
                name=filter_name,
                defaults={
                    'central_wl': central_wl,
                    'width': width,
                    'manufacturer': 'Default Manufacturer',
                    'uid': uuid.uuid4()
                }
            )
            
            if filter_created:
                self.stdout.write(self.style.SUCCESS(f'Created Filter: {filter_name}'))
                
            # Create or update filter position
            filter_position, fp_created = FilterPosition.objects.get_or_create(
                filter_wheel=filter_wheel,
                filter=filter_obj,
                defaults={'position': position, 'uid': uuid.uuid4()}
            )
            
            # Update position if changed
            if not fp_created and filter_position.position != position:
                FilterHistory.objects.create(
                    filter=filter_obj,
                    from_unit=filter_wheel.unit,
                    to_unit=filter_wheel.unit,
                    reason=f"Position changed from {filter_position.position} to {position}"
                )
                filter_position.position = position
                filter_position.save()
                self.stdout.write(self.style.SUCCESS(
                    f'Updated Filter: {filter_name} position to {position} in {filter_wheel.uid}'
                ))

    def _process_weather(self, weather_path):
        """Process weather data from weatherinfo.dict and update database"""
        try:
            if not os.path.exists(weather_path):
                self.stdout.write(self.style.WARNING(f"Weather file not found: {weather_path}"))
                return
            
            with open(weather_path, 'r') as f:
                weather_data = json.load(f)
           
            # Handle a single weather data record (weatherinfo.dict format)
            if isinstance(weather_data, dict):
                try:
                    # Get timestamp if available, or use current time
                    if 'update_time' in weather_data:
                        timestamp_str = weather_data['update_time']
                        # Parse datetime and check if timezone aware
                        dt = timezone.datetime.fromisoformat(timestamp_str)
                        timestamp = dt if dt.tzinfo else make_aware(dt)
                    else:
                        timestamp = timezone.now()

                    Weather.objects.create(
                        timestamp = timestamp,
                        temperature = weather_data.get('temperature'),
                        humidity = weather_data.get('humidity'),
                        pressure = weather_data.get('pressure'),
                        wind_speed = weather_data.get('windspeed'),
                        wind_direction = weather_data.get('winddirection'),
                        sky_temperature = weather_data.get('skytemperature'),
                        ambient_light = weather_data.get('skybrightness'),
                        cloud_cover = weather_data.get('cloudfraction'),
                        rain_rate = weather_data.get('rainrate'),
                        seeing = weather_data.get('fwhm'),
                        dew_point = weather_data.get('dewpoint'),
                        safe_status = weather_data.get('is_safe', True)
                    )

                    self.stdout.write(self.style.SUCCESS(
                        f'Weather data saved (T: {weather_data.get("temperature")}°C, '
                        f'H: {weather_data.get("humidity")}%, '
                        f'Wind: {weather_data.get("windspeed")} m/s)'
                    ))
                    return True

                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"Error processing weather data: {e}"))
                    self.stdout.write(traceback.format_exc())
                    return False
            else:
                self.stdout.write(self.style.ERROR(f"Unexpected weather data format"))
                return False

        except Exception as e:
           self.stdout.write(self.style.ERROR(f"Error processing weather data: {e}"))
           self.stdout.write(traceback.format_exc())
           return False
