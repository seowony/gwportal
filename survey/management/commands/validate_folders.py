from django.core.management.base import BaseCommand
from survey.models import Night

class Command(BaseCommand):
    help = 'Validate observation folder structure and identify non-compliant directories'

    def add_arguments(self, parser):
        parser.add_argument(
            '--base-path',
            default='/lyman/data1/obsdata',
            help='Base path containing observation unit directories (default: /lyman/data1/obsdata)',
        )
        parser.add_argument(
            '--detailed',
            action='store_true',
            help='Generate detailed validation report with all folder analysis',
        )
        parser.add_argument(
            '--invalid-only',
            action='store_true',
            help='Show only problematic folders (default behavior)',
        )

    def handle(self, *args, **options):
        base_path = options['base_path']
        
        if options['detailed']:
            self.stdout.write('Running comprehensive folder structure validation...')
            results = Night.validate_folder_structure(base_path)
        else:
            self.stdout.write('Identifying non-compliant folders...')
            results = Night.show_invalid_folders(base_path)
        
        # Display summary statistics
        stats = results['statistics']
        total_issues = (stats['invalid_date'] + stats['suspicious'] + 
                       stats['too_old'] + stats['future_date'])
        
        if total_issues == 0:
            self.stdout.write(
                self.style.SUCCESS('✅ All observation folders follow the expected naming convention!')
            )
        else:
            self.stdout.write(
                self.style.WARNING(f'⚠️  Found {total_issues} folders requiring attention')
            )
