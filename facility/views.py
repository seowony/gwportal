# views.py
from django.shortcuts import render
from django.http import JsonResponse
from bokeh.plotting import figure
from bokeh.embed import components
from bokeh.models import ColumnDataSource, HoverTool, Whisker
from bokeh.layouts import column, gridplot
import numpy as np
from facility.models import Unit, Camera, Filter, FilterWheel, FilterPosition
import logging

logger = logging.getLogger(__name__)

def gaussian(x, mu, sigma):
    """Normalized gaussian function."""
    y = np.exp(-0.5 * ((x - mu) / sigma) ** 2)
    return y / np.max(y)

def get_all_filters_data():
    filters = Filter.objects.all()
    x = np.linspace(300, 1100, 1000)
    data = {'x': x.tolist()}

    for f in filters:
        y = gaussian(x, f.central_wl, f.width / 2.)
        data[f'{f.name}_{f.id}'] = y.tolist()

    return data

def get_unit_filters_data(unit_name):
    try:
        unit = Unit.objects.get(name=unit_name)
        filter_wheel = FilterWheel.objects.filter(unit=unit).first()
        if not filter_wheel:
            return {'x': [], 'y': []}

        filter_positions = FilterPosition.objects.filter(
            filter_wheel=filter_wheel
        ).select_related('filter')

        x = np.linspace(300, 1100, 1000)
        data = {'x': x.tolist()}

        for fp in filter_positions:
            f = fp.filter
            if f and f.central_wl and f.width:
                y = gaussian(x, f.central_wl, f.width / 2.)
                data[f'{f.name}_{f.id}'] = y.tolist()

        return data
    except Unit.DoesNotExist:
        return {'x': [], 'y': []}
    except Exception as e:
        logger.error(f"Error in get_unit_filters_data: {str(e)}")
        return {'x': [], 'y': []}

def get_unit_info(unit_name):
    try:
        unit = Unit.objects.get(name=unit_name)
        camera = Camera.objects.filter(unit=unit).first()
        filter_wheel = FilterWheel.objects.filter(unit=unit).first()
        
        status_mapping = {
            'operational': 'active',
            'degraded': 'degraded',
            'maintenance': 'maintenance',
            'offline': 'inactive',
            'error': 'error'
        }
       
        # Unit status
        try:
            unit_status = unit.get_status()
            unit_status_display = unit_status.get_status_display() if unit_status else 'Unknown'
            unit_status_class = status_mapping.get(unit_status.status if unit_status else '', 'unknown')
        except:
            unit_status_display = 'Active' if unit.is_active else 'Inactive'
            unit_status_class = 'active' if unit.is_active else 'inactive'

        # Camera status
        try:
            camera_status = camera.get_status() if camera else None
            camera_status_display = camera_status.get_status_display() if camera_status else 'Unknown'
            camera_status_class = status_mapping.get(camera_status.status if camera_status else '', 'unknown')
        except:
            camera_status_display = 'Active' if (camera and camera.is_active) else 'Inactive'
            camera_status_class = 'active' if (camera and camera.is_active) else 'inactive'

        filter_info = []
        if filter_wheel:
            filter_positions = FilterPosition.objects.filter(
                filter_wheel=filter_wheel
            ).select_related('filter').order_by('position')
            
            for fp in filter_positions:
                if fp.filter:
                    filter_info.append({
                        'name': fp.filter.name,
                        'central_wl': float(fp.filter.central_wl),
                        'width': float(fp.filter.width),
                        'position': int(fp.position),
                    })

        return {
            'unit_status': unit_status_display,
            'unit_status_class': unit_status_class,
            'camera_model': camera.model if camera else 'N/A',
            'camera_status': camera_status_display,
            'camera_status_class': camera_status_class,
            'fw_model': filter_wheel.uid if filter_wheel else 'N/A',
            'filters': filter_info
        }
    except Unit.DoesNotExist:
        return None
    except Exception as e:
        print(f"Error in get_unit_info: {str(e)}") # for debugging 
        return None


def update_filters(request, unit_name):
    try:
        filter_data = get_unit_filters_data(unit_name)
        unit_info = get_unit_info(unit_name)
        
        if unit_info is None:
            unit_info = {
                'unit_status': 'Unknown',
                'unit_status_class': 'unknown',
                'camera_model': 'N/A',
                'camera_status': 'Unknown',
                'camera_status_class': 'unknown',
                'fw_model': 'N/A',
                'filters': []
            }
        
        return JsonResponse({
            'filter_data': filter_data,
            'unit_info': unit_info
        })
    except Exception as e:
        logger.error(f"Error in update_filters: {str(e)}")
        return JsonResponse({
            'filter_data': {'x': []},
            'unit_info': {
                'unit_status': 'Error',
                'unit_status_class': 'error',
                'camera_model': 'N/A',
                'camera_status': 'Unknown',
                'camera_status_class': 'unknown',
                'fw_model': 'N/A',
                'filters': []
            }
        })

def dashboard(request):
    units = Unit.objects.all()
    unit_names = [f'7DT{str(i).zfill(2)}' for i in range(1, 21)]

    # Status Plot
    unit_status = []
    camera_status = []
    for unit_name in unit_names:
        try:
            unit = Unit.objects.get(name=unit_name)
            unit_status.append('green' if unit.is_active else 'red')
            camera = Camera.objects.filter(unit=unit).first()
            camera_status.append('green' if (camera and camera.is_active) else 'red')
        except Unit.DoesNotExist:
            unit_status.append('yellow')
            camera_status.append('yellow')

    p1 = figure(x_range=unit_names, y_range=['Unit', 'Camera'],
                title="Unit and Camera Status", tools="",
                width=800, height=200)

    p1.rect(x=unit_names, y=['Unit'] * len(unit_names),
            width=0.8, height=0.8,
            color=unit_status, line_color="black")

    p1.rect(x=unit_names, y=['Camera'] * len(unit_names),
            width=0.8, height=0.8,
            color=camera_status, line_color="black")

    p1.xaxis.axis_label = "Units"
    p1.xgrid.grid_line_color = None
    p1.ygrid.grid_line_color = None

    # Filter Plot
    initial_unit = unit_names[0]  # First unit as default
    initial_unit_info = get_unit_info(initial_unit)
    if initial_unit_info is None:
        initial_unit_info = {
            'unit_status': 'Unknown',
            'unit_status_class': 'unknown',
            'camera_model': 'N/A',
            'camera_status': 'Unknown',
            'camera_status_class': 'unknown',
            'fw_model': 'N/A',
            'filters': []
        }

    initial_unit_filters_data = get_unit_filters_data(initial_unit)
    filter_source = ColumnDataSource(initial_unit_filters_data, name='filter_source')

    p2 = figure(title=f"Filter Transmission Curves - {initial_unit}",
                width=800, height=400,
                name='filter_plot')

    colors = [
        '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
        '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf',
    ]
    # Highlight initial unit filters with colors
    filter_keys = [k for k in initial_unit_filters_data.keys() if k != 'x']
    for i, key in enumerate(filter_keys):
        color_idx = i % len(colors)
        p2.line('x', key,
                line_color=colors[color_idx],
                source=filter_source,
                line_width=2,
                legend_label=key.split('_')[0],
                name=f'highlight_{i}')

    p2.xaxis.axis_label = "Wavelength (nm)"
    p2.yaxis.axis_label = "Normalized Transmission"
    p2.legend.click_policy = "hide"
    p2.legend.location = "top_right"

    # Combine plots
    layout = column([p1, p2])
    script, div = components(layout)

    return render(request, 'dashboard.html', {
        'script': script,
        'div': div,
        'unit_names': unit_names,
        'initial_unit': initial_unit,
        'initial_unit_info': initial_unit_info
    })
