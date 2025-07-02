from django.core.management.base import BaseCommand
from survey.models import Tile
from django.db.models import Min, Max, Count, Sum, Avg

class Command(BaseCommand):
    help = 'Test and validate tile data'

    def add_arguments(self, parser):
        parser.add_argument(
            '--count',
            type=int,
            default=10,
            help='Number of tiles to display in detail',
        )
        parser.add_argument(
            '--test-search',
            action='store_true',
            help='Test spatial search functionality',
        )

    def handle(self, *args, **options):
        self.show_tile_statistics()
        
        if options['count'] > 0:
            self.show_sample_tiles(options['count'])
        
        if options['test_search']:
            self.test_spatial_search()
    
    def show_tile_statistics(self):
        """Show overall tile statistics"""
        total = Tile.objects.count()
        self.stdout.write(f'\n=== TILE STATISTICS ===')
        self.stdout.write(f'Total tiles: {total}')
        
        if total == 0:
            self.stdout.write('No tiles found in database')
            return
        
        # Coordinate ranges
        stats = Tile.objects.aggregate(
            min_ra=Min('ra'),
            max_ra=Max('ra'),
            min_dec=Min('dec'),
            max_dec=Max('dec'),  # 이 줄에 쉼표 추가
            avg_area=Avg('area_sq_deg')
        )
        
        self.stdout.write(f'RA range: {stats["min_ra"]:.3f} to {stats["max_ra"]:.3f}')
        self.stdout.write(f'Dec range: {stats["min_dec"]:.3f} to {stats["max_dec"]:.3f}')
        self.stdout.write(f'Average area: {stats["avg_area"]:.3f} sq deg')
        
        # ID ranges
        id_stats = Tile.objects.aggregate(
            min_id=Min('id'),
            max_id=Max('id')
        )
        self.stdout.write(f'Tile ID range: {id_stats["min_id"]} to {id_stats["max_id"]}')
    
    def show_sample_tiles(self, count):
        """Show sample tiles with details"""
        self.stdout.write(f'\n=== SAMPLE TILES (first {count}) ===')
        
        tiles = Tile.objects.order_by('id')[:count]
        
        for tile in tiles:
            vertices = tile.vertex_coords
            self.stdout.write(
                f'{tile.name}: RA={tile.ra:.3f}, Dec={tile.dec:.3f}, '
                f'Area={tile.area_sq_deg:.3f} sq deg'
            )
            self.stdout.write(f'  Vertices: {len(vertices)} points')
            for i, (ra, dec) in enumerate(vertices[:4]):  # Show first 4 vertices
                self.stdout.write(f'    {i+1}: ({ra:.3f}, {dec:.3f})')
    
    def test_spatial_search(self):
        """Test spatial search functionality"""
        self.stdout.write(f'\n=== TESTING SPATIAL SEARCH ===')
        
        # Test point containment
        test_points = [
            (0.0, -90.0),    # South pole
            (180.0, 0.0),    # Equator
            (270.0, 45.0),   # North
        ]
        
        for ra, dec in test_points:
            try:
                # Test the Q3C radial search
                tiles = Tile.q3c_radial_search(ra, dec, 1.0)  # 1 degree radius
                count = tiles.count()
                self.stdout.write(f'Point ({ra}, {dec}): Found {count} tiles within 1 degree')
                
                if count > 0:
                    closest = tiles.first()
                    self.stdout.write(f'  Closest: {closest.name} at ({closest.ra:.3f}, {closest.dec:.3f})')
                    
            except Exception as e:
                self.stdout.write(f'  Error testing point ({ra}, {dec}): {e}')
