import os
from django.core.management.base import BaseCommand, CommandError
from survey.models import Tile
from django.db.models import Min, Max, Count, Sum, Avg

class Command(BaseCommand):
    help = 'Load tile information from final_tiles.txt file'

    def add_arguments(self, parser):
        parser.add_argument(
            '--file',
            #default='/lyman/data1/obsdata/final_tiles.txt',
            default='/home/db/gwportal/survey/management/commands/final_tiles_extended.txt',
            help='Path to final_tiles.txt file',
        )
        parser.add_argument(
            '--clear',
            action='store_true',
            help='Clear all existing tiles before loading',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be loaded without actually loading',
        )

    def handle(self, *args, **options):
        file_path = options['file']
        
        # Check if file exists
        if not os.path.exists(file_path):
            raise CommandError(f'File does not exist: {file_path}')
        
        # Clear existing tiles if requested
        if options['clear']:
            if options['dry_run']:
                count = Tile.objects.count()
                self.stdout.write(f'Would delete {count} existing tiles')
            else:
                count = Tile.objects.count()
                Tile.objects.all().delete()
                self.stdout.write(
                    self.style.WARNING(f'Deleted {count} existing tiles')
                )
        
        # Load tiles
        if options['dry_run']:
            self.dry_run_load(file_path)
        else:
            self.load_tiles(file_path)
    
    def dry_run_load(self, file_path):
        """Show what would be loaded without actually loading"""
        import csv
        
        count = 0
        self.stdout.write(f'DRY RUN: Reading from {file_path}')
        
        try:
            with open(file_path, 'r') as f:
                reader = csv.reader(f, delimiter=' ', skipinitialspace=True)
                header = next(reader)
                self.stdout.write(f'Header: {" ".join(header)}')
                
                for i, row in enumerate(reader):
                    if not row:
                        continue
                    
                    count += 1
                    if count <= 5:  # Show first 5 examples
                        tile_id = int(row[0][1:]) if row[0].startswith('T') else int(row[0])
                        ra = float(row[1])
                        dec = float(row[2])
                        self.stdout.write(f'  Would load: T{tile_id:05d} (RA={ra:.3f}, Dec={dec:.3f})')
                
                self.stdout.write(f'Total tiles that would be loaded: {count}')
                
        except Exception as e:
            raise CommandError(f'Error reading file: {e}')
    
    def load_tiles(self, file_path):
        """Actually load the tiles"""
        try:
            result = Tile.load_from_file(file_path)
            
            self.stdout.write(
                self.style.SUCCESS(
                    f'Successfully loaded tiles: '
                    f'{result["created"]} created, '
                    f'{result["updated"]} updated, '
                    f'{result["total"]} total'
                )
            )
            
            # Show some statistics
            total_tiles = Tile.objects.count()
            ra_range = Tile.objects.aggregate(
                min_ra=Min('ra'),
                max_ra=Max('ra'),
                min_dec=Min('dec'),
                max_dec=Max('dec')
            )
            
            self.stdout.write(f'Database now contains {total_tiles} tiles')
            self.stdout.write(f'RA range: {ra_range["min_ra"]:.1f} to {ra_range["max_ra"]:.1f}')
            self.stdout.write(f'Dec range: {ra_range["min_dec"]:.1f} to {ra_range["max_dec"]:.1f}')
            
        except Exception as e:
            raise CommandError(f'Error loading tiles: {e}')
