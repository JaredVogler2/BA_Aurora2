import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from collections import defaultdict, deque
import heapq
from typing import Dict, List, Set, Tuple, Optional
import warnings
import copy

warnings.filterwarnings('ignore')

"""
Enhanced Production Scheduler with Product-Specific Late Parts and Rework

Expected data format for product-specific tasks:

==== LATE PARTS RELATIONSHIPS TABLE ====
First,Second,Estimated On Dock Date,Product Line
301,2,8/30/2025,Product E
302,18,9/2/2025,Product D
...

==== REWORK RELATIONSHIPS TABLE ====
First,Second,Relationship Type,Product Line
401,10,Finish <= Start,Product E
402,401,Finish <= Start,Product E
...

These product associations ensure that late parts and rework only affect 
their specific product's schedule and priority calculations.
"""


class ProductionScheduler:
    """
    Enhanced Production scheduling system that generates a global prioritized task list
    while respecting all constraints and minimizing delivery lateness.
    Supports concurrent task execution within team capacity limits.
    Includes Late Part tasks and Rework tasks with their specific constraints.

    Product-Specific Task Handling:
    - Late Part tasks are associated with specific products via the LATE PARTS RELATIONSHIPS TABLE
    - Rework tasks are associated with specific products via the REWORK RELATIONSHIPS TABLE
    - Each late part/rework only affects its associated product's schedule
    - Priority calculations consider the specific product's delivery date
    - Resource optimization accounts for product-specific bottlenecks

    Three analysis scenarios:
    - Scenario 1: Schedule with CSV-defined headcount (allowing late delivery if needed)
    - Scenario 2: Find minimum headcount to minimize makespan
    - Scenario 3: Find minimum headcount to meet all delivery dates
        - 3A: Uniform capacity across teams
        - 3B: Multi-dimensional optimization per team
    """

    def __init__(self, csv_file_path='scheduling_data.csv', debug=False, late_part_delay_days=1.0):
        """
        Initialize scheduler with CSV file containing all tables.
        The CSV should contain sections separated by ==== markers.

        Args:
            csv_file_path: Path to the CSV file with scheduling data
            debug: Enable verbose debug output
            late_part_delay_days: Days after on-dock date before late part task can start (default 1.0)
        """
        self.csv_path = csv_file_path
        self.debug = debug
        self.late_part_delay_days = late_part_delay_days  # Parameterizable delay for late parts

        self.tasks = {}
        self.quality_inspections = {}
        self.quality_requirements = {}
        self.precedence_constraints = []
        self.late_part_constraints = []  # New: Store late part relationships
        self.rework_constraints = []  # New: Store rework relationships
        self.late_part_tasks = {}  # New: Store late part task details
        self.rework_tasks = {}  # New: Store rework task details
        self.on_dock_dates = {}  # New: Store on-dock dates for late part tasks
        self.task_to_product = {}  # New: Map tasks to their product lines

        self.team_shifts = {}
        self.team_capacity = {}
        self.quality_team_shifts = {}
        self.quality_team_capacity = {}
        self.shift_hours = {}
        self.delivery_dates = {}
        self.holidays = defaultdict(set)
        self.product_tasks = defaultdict(list)
        self.task_schedule = {}
        self.global_priority_list = []
        self._dynamic_constraints_cache = None
        self._critical_path_cache = {}

        # Store original capacities for reset
        self._original_team_capacity = {}
        self._original_quality_capacity = {}

    def debug_print(self, message, force=False):
        """Print debug message if debug mode is enabled or forced"""
        if self.debug or force:
            print(message)

    def parse_csv_sections(self, file_content):
        """Parse CSV file content into separate sections based on ==== markers"""
        sections = {}
        current_section = None
        current_data = []

        for line in file_content.strip().split('\n'):
            if '====' in line and line.strip().startswith('===='):
                if current_section and current_data:
                    sections[current_section] = '\n'.join(current_data)
                    if self.debug:
                        print(f"[DEBUG] Saved section '{current_section}' with {len(current_data)} lines")
                current_section = line.replace('=', '').strip()
                current_data = []
            else:
                if line.strip():
                    current_data.append(line)

        if current_section and current_data:
            sections[current_section] = '\n'.join(current_data)
            if self.debug:
                print(f"[DEBUG] Saved section '{current_section}' with {len(current_data)} lines")

        if self.debug:
            print("\n[DEBUG] Section contents preview:")
            for name, content in sections.items():
                print(f"  '{name}': {repr(content[:100])}...")

        return sections

    def load_data_from_csv(self):
        """Load all data from the CSV file containing all tables"""
        print(f"\n[DEBUG] Starting to load data from {self.csv_path}")

        # Clear any cached data
        self._dynamic_constraints_cache = None
        self._critical_path_cache = {}

        # Read the CSV file
        try:
            with open(self.csv_path, 'r', encoding='utf-8') as f:
                content = f.read()
        except UnicodeDecodeError:
            print("[WARNING] UTF-8 decoding failed, trying latin-1...")
            with open(self.csv_path, 'r', encoding='latin-1') as f:
                content = f.read()

        # Remove BOM if present
        if content.startswith('\ufeff'):
            print("[WARNING] Removing BOM from file")
            content = content[1:]

        print(f"[DEBUG] Read {len(content)} characters from CSV file")

        sections = self.parse_csv_sections(content)
        print(f"[DEBUG] Found {len(sections)} sections in CSV file")
        print(f"[DEBUG] Section names found: {list(sections.keys())}")

        # Load Task Relationships (baseline tasks)
        if "TASK RELATIONSHIPS TABLE" in sections:
            from io import StringIO
            df = pd.read_csv(StringIO(sections["TASK RELATIONSHIPS TABLE"]))
            # Strip whitespace from column names
            df.columns = df.columns.str.strip()
            for col in ['First', 'Second']:
                if col in df.columns:
                    df[col] = df[col].astype(int)
            self.precedence_constraints = df.to_dict('records')
            print(f"[DEBUG] Loaded {len(self.precedence_constraints)} task relationships")

        # Load Task Duration and Resources
        if "TASK DURATION AND RESOURCE TABLE" in sections:
            from io import StringIO
            df = pd.read_csv(StringIO(sections["TASK DURATION AND RESOURCE TABLE"]))
            # Strip whitespace from column names
            df.columns = df.columns.str.strip()
            task_count = 0
            for _, row in df.iterrows():
                try:
                    task_id = int(row['Task'])
                    # Check if all required columns are present
                    if pd.isna(row.get('Duration (minutes)')) or pd.isna(row.get('Resource Type')) or pd.isna(
                            row.get('Mechanics Required')):
                        print(f"[WARNING] Skipping incomplete task row: {row}")
                        continue

                    self.tasks[task_id] = {
                        'duration': int(row['Duration (minutes)']),
                        'team': row['Resource Type'].strip(),  # Also strip team names
                        'mechanics_required': int(row['Mechanics Required']),
                        'is_quality': False,
                        'task_type': 'Production'  # Default type
                    }
                    task_count += 1
                except (ValueError, KeyError) as e:
                    print(f"[WARNING] Error processing task row: {row}, Error: {e}")
                    continue
            print(f"[DEBUG] Loaded {task_count} production tasks")

        # Load Late Parts Relationships and Task Details
        if "LATE PARTS RELATIONSHIPS TABLE" in sections:
            from io import StringIO
            df = pd.read_csv(StringIO(sections["LATE PARTS RELATIONSHIPS TABLE"]))
            # Strip whitespace from column names to handle tabs/spaces
            df.columns = df.columns.str.strip()
            lp_count = 0
            has_product_column = 'Product Line' in df.columns

            if not has_product_column:
                print(f"[WARNING] No 'Product Line' column in LATE PARTS RELATIONSHIPS TABLE")
                print(f"[WARNING] Late parts will be associated with products based on dependent tasks")

            for _, row in df.iterrows():
                try:
                    first_task = int(row['First'])
                    second_task = int(row['Second'])
                    on_dock_date = pd.to_datetime(row['Estimated On Dock Date'])
                    product_line = row['Product Line'].strip() if has_product_column and pd.notna(
                        row.get('Product Line')) else None

                    # Store the constraint
                    self.late_part_constraints.append({
                        'First': first_task,
                        'Second': second_task,
                        'Relationship': 'Finish <= Start',
                        'On_Dock_Date': on_dock_date,
                        'Product_Line': product_line
                    })

                    # Store on-dock date for the late part task
                    self.on_dock_dates[first_task] = on_dock_date

                    # Store product association for the late part task
                    if product_line:
                        self.task_to_product[first_task] = product_line

                    lp_count += 1
                except (ValueError, KeyError) as e:
                    print(f"[WARNING] Error processing late part relationship row: {row}, Error: {e}")
                    continue
            print(f"[DEBUG] Loaded {lp_count} late part relationships")

            # Show product associations if available
            if has_product_column and self.late_part_constraints:
                product_counts = defaultdict(int)
                for lp in self.late_part_constraints:
                    if lp.get('Product_Line'):
                        product_counts[lp['Product_Line']] += 1
                if product_counts:
                    print(f"[DEBUG] Late parts by product:")
                    for product, count in sorted(product_counts.items()):
                        print(f"  - {product}: {count} late parts")

        # Load Late Parts Task Details (duration and resources)
        if "LATE PARTS TASK DETAILS" in sections:
            from io import StringIO
            df = pd.read_csv(StringIO(sections["LATE PARTS TASK DETAILS"]))
            # Strip whitespace from column names
            df.columns = df.columns.str.strip()
            lp_task_count = 0
            for _, row in df.iterrows():
                try:
                    task_id = int(row['Task'])
                    # Check if all required columns are present
                    if pd.isna(row.get('Duration (minutes)')) or pd.isna(row.get('Resource Type')) or pd.isna(
                            row.get('Mechanics Required')):
                        print(f"[WARNING] Skipping incomplete late part task row: {row}")
                        continue

                    self.tasks[task_id] = {
                        'duration': int(row['Duration (minutes)']),
                        'team': row['Resource Type'].strip(),  # Strip team names
                        'mechanics_required': int(row['Mechanics Required']),
                        'is_quality': False,
                        'task_type': 'Late Part'  # Mark as Late Part
                    }
                    self.late_part_tasks[task_id] = True
                    lp_task_count += 1
                except (ValueError, KeyError) as e:
                    print(f"[WARNING] Error processing late part task row: {row}, Error: {e}")
                    continue
            print(f"[DEBUG] Loaded {lp_task_count} late part task details")

        # Load Rework Relationships
        if "REWORK RELATIONSHIPS TABLE" in sections:
            from io import StringIO
            df = pd.read_csv(StringIO(sections["REWORK RELATIONSHIPS TABLE"]))
            # Strip whitespace from column names
            df.columns = df.columns.str.strip()
            rw_count = 0
            has_product_column = 'Product Line' in df.columns

            if not has_product_column:
                print(f"[WARNING] No 'Product Line' column in REWORK RELATIONSHIPS TABLE")
                print(f"[WARNING] Rework tasks will be associated with products based on dependent tasks")

            for _, row in df.iterrows():
                try:
                    first_task = int(row['First'])
                    second_task = int(row['Second'])
                    product_line = row['Product Line'].strip() if has_product_column and pd.notna(
                        row.get('Product Line')) else None

                    self.rework_constraints.append({
                        'First': first_task,
                        'Second': second_task,
                        'Relationship': row.get('Relationship Type', 'Finish <= Start').strip() if pd.notna(
                            row.get('Relationship Type')) else 'Finish <= Start',
                        'Product_Line': product_line
                    })

                    # Store product association for the rework task
                    if product_line:
                        self.task_to_product[first_task] = product_line

                    rw_count += 1
                except (ValueError, KeyError) as e:
                    print(f"[WARNING] Error processing rework relationship row: {row}, Error: {e}")
                    continue
            print(f"[DEBUG] Loaded {rw_count} rework relationships")

            # Show product associations if available
            if has_product_column and self.rework_constraints:
                product_counts = defaultdict(int)
                for rw in self.rework_constraints:
                    if rw.get('Product_Line'):
                        product_counts[rw['Product_Line']] += 1
                if product_counts:
                    print(f"[DEBUG] Rework by product:")
                    for product, count in sorted(product_counts.items()):
                        print(f"  - {product}: {count} rework tasks")

        # Load Rework Task Details
        if "REWORK TASK DETAILS" in sections:
            from io import StringIO
            df = pd.read_csv(StringIO(sections["REWORK TASK DETAILS"]))
            # Strip whitespace from column names
            df.columns = df.columns.str.strip()
            rw_task_count = 0
            for _, row in df.iterrows():
                try:
                    task_id = int(row['Task'])
                    # Check if all required columns are present
                    if pd.isna(row.get('Duration (minutes)')) or pd.isna(row.get('Resource Type')) or pd.isna(
                            row.get('Mechanics Required')):
                        print(f"[WARNING] Skipping incomplete rework task row: {row}")
                        continue

                    self.tasks[task_id] = {
                        'duration': int(row['Duration (minutes)']),
                        'team': row['Resource Type'].strip(),  # Strip team names
                        'mechanics_required': int(row['Mechanics Required']),
                        'is_quality': False,
                        'task_type': 'Rework'  # Mark as Rework
                    }
                    self.rework_tasks[task_id] = True
                    rw_task_count += 1

                    # Create quality inspection for rework tasks (they need QI too)
                    qi_task_id = task_id + 10000  # Offset to avoid conflicts
                    self.quality_requirements[task_id] = qi_task_id

                    # Create the quality inspection task for rework
                    self.tasks[qi_task_id] = {
                        'duration': 30,  # Default QI duration for rework
                        'team': None,  # Will be assigned based on shift availability
                        'mechanics_required': 1,  # Default QI headcount
                        'is_quality': True,
                        'task_type': 'Quality Inspection',
                        'primary_task': task_id
                    }

                    self.quality_inspections[qi_task_id] = {
                        'primary_task': task_id,
                        'headcount': 1
                    }

                    # If this rework task has a product association, also associate its QI
                    if task_id in self.task_to_product:
                        self.task_to_product[qi_task_id] = self.task_to_product[task_id]

                except (ValueError, KeyError) as e:
                    print(f"[WARNING] Error processing rework task row: {row}, Error: {e}")
                    continue

            print(f"[DEBUG] Loaded {rw_task_count} rework task details")
            print(f"[DEBUG] Created {rw_task_count} quality inspections for rework tasks")

        # Load Quality Inspection Requirements (for baseline production tasks)
        if "QUALITY INSPECTION REQUIREMENTS" in sections:
            from io import StringIO
            df = pd.read_csv(StringIO(sections["QUALITY INSPECTION REQUIREMENTS"]))
            # Strip whitespace from column names
            df.columns = df.columns.str.strip()
            qi_count = 0
            for _, row in df.iterrows():
                primary_task = int(row['Primary Task'])
                qi_task = int(row['Quality Task'])

                self.quality_requirements[primary_task] = qi_task

                # Create the quality inspection task
                self.tasks[qi_task] = {
                    'duration': int(row['Quality Duration (minutes)']),
                    'team': None,  # Will be assigned based on shift availability
                    'mechanics_required': int(row['Quality Headcount Required']),
                    'is_quality': True,
                    'task_type': 'Quality Inspection',
                    'primary_task': primary_task
                }

                # Store quality inspection info
                self.quality_inspections[qi_task] = {
                    'primary_task': primary_task,
                    'headcount': int(row['Quality Headcount Required'])
                }
                qi_count += 1
            print(f"[DEBUG] Loaded {qi_count} quality inspection requirements for baseline tasks")
            print(f"[DEBUG] Total tasks now: {len(self.tasks)}")

        # Load Mechanic Team Working Calendars
        if "MECHANIC TEAM WORKING CALENDARS" in sections:
            from io import StringIO
            df = pd.read_csv(StringIO(sections["MECHANIC TEAM WORKING CALENDARS"]))
            df.columns = df.columns.str.strip()
            for _, row in df.iterrows():
                team_name = row['Mechanic Team'].strip()
                shifts = row['Working Shifts']
                if 'All 3 shifts' in shifts:
                    self.team_shifts[team_name] = ['1st', '2nd', '3rd']
                elif 'and' in shifts:
                    self.team_shifts[team_name] = [s.strip() for s in shifts.split('and')]
                else:
                    self.team_shifts[team_name] = [shifts.strip()]
            print(f"[DEBUG] Loaded {len(self.team_shifts)} mechanic team schedules")

        # Load Quality Team Working Calendars
        if "QUALITY TEAM WORKING CALENDARS" in sections:
            from io import StringIO
            df = pd.read_csv(StringIO(sections["QUALITY TEAM WORKING CALENDARS"]))
            df.columns = df.columns.str.strip()
            for _, row in df.iterrows():
                team_name = row['Quality Team'].strip()
                self.quality_team_shifts[team_name] = [row['Working Shifts'].strip()]
            print(f"[DEBUG] Loaded {len(self.quality_team_shifts)} quality team schedules")

        # Load Shift Working Hours
        if "SHIFT WORKING HOURS" in sections:
            from io import StringIO
            df = pd.read_csv(StringIO(sections["SHIFT WORKING HOURS"]))
            df.columns = df.columns.str.strip()
            for _, row in df.iterrows():
                self.shift_hours[row['Shift'].strip()] = {
                    'start': row['Start Time'].strip(),
                    'end': row['End Time'].strip()
                }
            print(f"[DEBUG] Loaded {len(self.shift_hours)} shift definitions")

        # Load Mechanic Team Capacity
        if "MECHANIC TEAM CAPACITY" in sections:
            from io import StringIO
            df = pd.read_csv(StringIO(sections["MECHANIC TEAM CAPACITY"]))
            df.columns = df.columns.str.strip()
            for _, row in df.iterrows():
                team_name = row['Mechanic Team'].strip()
                capacity = int(row['Total Capacity (People)'])
                self.team_capacity[team_name] = capacity
                self._original_team_capacity[team_name] = capacity
            print(f"[DEBUG] Loaded capacity for {len(self.team_capacity)} mechanic teams")

        # Load Quality Team Capacity
        if "QUALITY TEAM CAPACITY" in sections:
            from io import StringIO
            df = pd.read_csv(StringIO(sections["QUALITY TEAM CAPACITY"]))
            df.columns = df.columns.str.strip()
            for _, row in df.iterrows():
                team_name = row['Quality Team'].strip()
                capacity = int(row['Total Capacity (People)'])
                self.quality_team_capacity[team_name] = capacity
                self._original_quality_capacity[team_name] = capacity
            print(f"[DEBUG] Loaded capacity for {len(self.quality_team_capacity)} quality teams")

        # Load Product Line Delivery Schedule
        if "PRODUCT LINE DELIVERY SCHEDULE" in sections:
            from io import StringIO
            df = pd.read_csv(StringIO(sections["PRODUCT LINE DELIVERY SCHEDULE"]))
            df.columns = df.columns.str.strip()
            for _, row in df.iterrows():
                product = row['Product Line'].strip()
                self.delivery_dates[product] = pd.to_datetime(row['Delivery Date'])
            print(f"[DEBUG] Loaded delivery dates for {len(self.delivery_dates)} product lines")

        # Load Product Line Jobs
        if "PRODUCT LINE JOBS" in sections:
            from io import StringIO
            df = pd.read_csv(StringIO(sections["PRODUCT LINE JOBS"]))
            df.columns = df.columns.str.strip()
            for _, row in df.iterrows():
                product = row['Product Line'].strip()
                task_count = 0
                start = int(row['Task Start'])
                end = int(row['Task End'])
                for task_id in range(start, end + 1):
                    self.product_tasks[product].append(task_id)
                    task_count += 1
                    # Also add quality inspection tasks for this product
                    if task_id in self.quality_requirements:
                        qi_task = self.quality_requirements[task_id]
                        self.product_tasks[product].append(qi_task)
                        task_count += 1

                # Add late part tasks that are specifically for this product
                for lp_constraint in self.late_part_constraints:
                    if lp_constraint.get('Product_Line') == product:
                        # Explicit product association
                        lp_task = lp_constraint['First']
                        if lp_task not in self.product_tasks[product]:
                            self.product_tasks[product].append(lp_task)
                            task_count += 1
                    elif not lp_constraint.get('Product_Line'):
                        # No explicit product - infer from dependent task
                        second_task = lp_constraint['Second']
                        if start <= second_task <= end:
                            lp_task = lp_constraint['First']
                            if lp_task not in self.product_tasks[product]:
                                self.product_tasks[product].append(lp_task)
                                self.task_to_product[lp_task] = product  # Store inferred association
                                task_count += 1

                # Add rework tasks that are specifically for this product
                for rw_constraint in self.rework_constraints:
                    if rw_constraint.get('Product_Line') == product:
                        # Explicit product association
                        rw_task = rw_constraint['First']
                        if rw_task not in self.product_tasks[product]:
                            self.product_tasks[product].append(rw_task)
                            task_count += 1
                            # Also add QI for rework task
                            if rw_task in self.quality_requirements:
                                qi_task = self.quality_requirements[rw_task]
                                if qi_task not in self.product_tasks[product]:
                                    self.product_tasks[product].append(qi_task)
                                    self.task_to_product[qi_task] = product
                                    task_count += 1
                    elif not rw_constraint.get('Product_Line'):
                        # No explicit product - infer from dependent task
                        second_task = rw_constraint['Second']
                        # Check if second task is in this product's range or is another rework task
                        if (start <= second_task <= end) or (second_task in self.rework_tasks):
                            rw_task = rw_constraint['First']
                            if rw_task not in self.product_tasks[product]:
                                self.product_tasks[product].append(rw_task)
                                self.task_to_product[rw_task] = product  # Store inferred association
                                task_count += 1
                                # Also add QI for rework task
                                if rw_task in self.quality_requirements:
                                    qi_task = self.quality_requirements[rw_task]
                                    if qi_task not in self.product_tasks[product]:
                                        self.product_tasks[product].append(qi_task)
                                        self.task_to_product[qi_task] = product
                                        task_count += 1

                if self.debug:
                    print(f"[DEBUG]   Product {product}: {task_count} tasks (including QI, late parts, rework)")

        # Load Holiday Calendar
        if "PRODUCT LINE HOLIDAY CALENDAR" in sections:
            from io import StringIO
            df = pd.read_csv(StringIO(sections["PRODUCT LINE HOLIDAY CALENDAR"]))
            df.columns = df.columns.str.strip()
            holiday_count = 0
            for _, row in df.iterrows():
                product = row['Product Line'].strip()
                self.holidays[product].add(pd.to_datetime(row['Date']))
                holiday_count += 1
            print(f"[DEBUG] Loaded {holiday_count} holiday entries")

        # Summary of task types
        print(f"\n[DEBUG] Task Type Summary:")
        task_type_counts = defaultdict(int)
        for task_info in self.tasks.values():
            task_type_counts[task_info['task_type']] += 1
        for task_type, count in sorted(task_type_counts.items()):
            print(f"  - {task_type}: {count} tasks")

        # Product-specific task breakdown
        print(f"\n[DEBUG] Product-Specific Task Breakdown:")
        for product in sorted(self.product_tasks.keys()):
            tasks_in_product = self.product_tasks[product]
            type_counts = defaultdict(int)
            for task_id in tasks_in_product:
                if task_id in self.tasks:
                    type_counts[self.tasks[task_id]['task_type']] += 1

            print(f"  {product}: {len(tasks_in_product)} total tasks")
            for task_type, count in sorted(type_counts.items()):
                print(f"    - {task_type}: {count}")

        print(f"[DEBUG] Data loading complete!")

    def build_dynamic_dependencies(self):
        """Build dependency graph with dynamic quality inspection insertion and late part/rework constraints"""
        if self._dynamic_constraints_cache is not None:
            return self._dynamic_constraints_cache

        self.debug_print(f"\n[DEBUG] Building dynamic dependencies...")
        self.debug_print(f"[DEBUG] Original constraints: {len(self.precedence_constraints)}")
        self.debug_print(f"[DEBUG] Late part constraints: {len(self.late_part_constraints)}")
        self.debug_print(f"[DEBUG] Rework constraints: {len(self.rework_constraints)}")
        self.debug_print(f"[DEBUG] Quality requirements: {len(self.quality_requirements)}")

        dynamic_constraints = []

        # 1. Add baseline task constraints with QI redirection
        qi_redirections = 0
        for constraint in self.precedence_constraints:
            first_task = constraint['First']
            second_task = constraint['Second']
            relationship = constraint.get('Relationship Type') or constraint.get('Relationship')

            if not relationship:
                print(f"[ERROR] No relationship type found in constraint: {constraint}")
                continue

            # Check if first task has quality inspection
            if first_task in self.quality_requirements:
                qi_task = self.quality_requirements[first_task]
                qi_redirections += 1

                # Add constraint from primary task to QI (Finish = Start)
                if not any(c['First'] == first_task and c['Second'] == qi_task
                           for c in dynamic_constraints):
                    dynamic_constraints.append({
                        'First': first_task,
                        'Second': qi_task,
                        'Relationship': 'Finish = Start'
                    })

                # Redirect original constraint through QI
                dynamic_constraints.append({
                    'First': qi_task,
                    'Second': second_task,
                    'Relationship': relationship
                })
            else:
                # No QI, keep original constraint
                dynamic_constraints.append({
                    'First': first_task,
                    'Second': second_task,
                    'Relationship': relationship
                })

        # 2. Add late part constraints
        lp_by_product = defaultdict(int)
        for lp_constraint in self.late_part_constraints:
            first_task = lp_constraint['First']
            second_task = lp_constraint['Second']
            product = lp_constraint.get('Product_Line', 'Unknown')
            lp_by_product[product] += 1

            # Late part must finish before primary task starts
            dynamic_constraints.append({
                'First': first_task,
                'Second': second_task,
                'Relationship': 'Finish <= Start',
                'Type': 'Late Part',
                'Product_Line': product
            })

            if self.debug and len(self.late_part_constraints) <= 5:
                print(f"[DEBUG] Added late part constraint: Task {first_task} -> Task {second_task} ({product})")

        if lp_by_product and self.debug:
            print(f"[DEBUG] Late part constraints by product: {dict(lp_by_product)}")

        # 3. Add rework constraints (including their QI)
        rw_by_product = defaultdict(int)
        for rw_constraint in self.rework_constraints:
            first_task = rw_constraint['First']
            second_task = rw_constraint['Second']
            relationship = rw_constraint.get('Relationship', 'Finish <= Start')
            product = rw_constraint.get('Product_Line', 'Unknown')
            rw_by_product[product] += 1

            # If rework task has QI, redirect through it
            if first_task in self.quality_requirements:
                qi_task = self.quality_requirements[first_task]

                # Add constraint from rework task to its QI
                if not any(c['First'] == first_task and c['Second'] == qi_task
                           for c in dynamic_constraints):
                    dynamic_constraints.append({
                        'First': first_task,
                        'Second': qi_task,
                        'Relationship': 'Finish = Start',
                        'Type': 'Rework QI',
                        'Product_Line': product
                    })

                # Redirect constraint through QI
                dynamic_constraints.append({
                    'First': qi_task,
                    'Second': second_task,
                    'Relationship': relationship,
                    'Type': 'Rework',
                    'Product_Line': product
                })
            else:
                # No QI, direct constraint
                dynamic_constraints.append({
                    'First': first_task,
                    'Second': second_task,
                    'Relationship': relationship,
                    'Type': 'Rework',
                    'Product_Line': product
                })

            if self.debug and len(self.rework_constraints) <= 5:
                print(f"[DEBUG] Added rework constraint: Task {first_task} -> Task {second_task} ({product})")

        if rw_by_product and self.debug:
            print(f"[DEBUG] Rework constraints by product: {dict(rw_by_product)}")

        # 4. Add any QI constraints that weren't already added
        added_qi_constraints = 0
        for primary_task, qi_task in self.quality_requirements.items():
            if not any(c['First'] == primary_task and c['Second'] == qi_task
                       for c in dynamic_constraints):
                dynamic_constraints.append({
                    'First': primary_task,
                    'Second': qi_task,
                    'Relationship': 'Finish = Start'
                })
                added_qi_constraints += 1

        self.debug_print(f"[DEBUG] QI redirections: {qi_redirections}")
        self.debug_print(f"[DEBUG] Additional QI constraints added: {added_qi_constraints}")
        self.debug_print(f"[DEBUG] Total dynamic constraints: {len(dynamic_constraints)}")

        self._dynamic_constraints_cache = dynamic_constraints
        return dynamic_constraints

    def get_earliest_start_for_late_part(self, task_id):
        """Calculate earliest start time for a late part task based on on-dock date"""
        if task_id not in self.on_dock_dates:
            return datetime(2025, 8, 22, 6, 0)  # Default start date if not a late part

        on_dock_date = self.on_dock_dates[task_id]
        # Add the parameterizable delay (default 1 day)
        earliest_start = on_dock_date + timedelta(days=self.late_part_delay_days)

        # Set to start of workday (6 AM)
        earliest_start = earliest_start.replace(hour=6, minute=0, second=0, microsecond=0)

        return earliest_start

    def schedule_tasks(self, allow_late_delivery=False, silent_mode=False):
        """Enhanced scheduling algorithm with capacity awareness and late part/rework handling

        Args:
            allow_late_delivery: If True, continue scheduling even if delivery dates are missed
            silent_mode: If True, suppress most debug output (for optimization scenarios)
        """
        # Save original debug setting
        original_debug = self.debug
        if silent_mode:
            self.debug = False

        # Clear previous schedule
        self.task_schedule = {}
        self._critical_path_cache = {}

        # Validate DAG first
        if not silent_mode and not self.validate_dag():
            raise ValueError("DAG validation failed! Cannot proceed with scheduling.")

        # Build dynamic dependencies including quality inspections, late parts, and rework
        dynamic_constraints = self.build_dynamic_dependencies()

        # Initialize start date
        start_date = datetime(2025, 8, 22, 6, 0)  # Start at 6 AM

        # Create dependency graph
        dependencies = defaultdict(set)
        dependents = defaultdict(set)

        for constraint in dynamic_constraints:
            if constraint['Relationship'] in ['Finish <= Start', 'Finish = Start']:
                dependencies[constraint['Second']].add(constraint['First'])
                dependents[constraint['First']].add(constraint['Second'])
            elif constraint['Relationship'] == 'Start <= Start':
                dependencies[constraint['Second']].add(constraint['First'])
                dependents[constraint['First']].add(constraint['Second'])

        # Find tasks with no dependencies (can start immediately)
        all_tasks = set(self.tasks.keys())
        total_tasks = len(all_tasks)
        ready_tasks = []

        if not silent_mode:
            print(f"\nStarting scheduling for {total_tasks} total tasks...")
            task_type_counts = defaultdict(int)
            for task_id in all_tasks:
                task_type_counts[self.tasks[task_id]['task_type']] += 1
            for task_type, count in sorted(task_type_counts.items()):
                print(f"- {task_type}: {count} tasks")

        for task in all_tasks:
            if task not in dependencies or len(dependencies[task]) == 0:
                priority = self.calculate_task_priority(task)
                heapq.heappush(ready_tasks, (priority, task))

        if not silent_mode:
            print(f"- Initial ready tasks: {len(ready_tasks)}")

        # Schedule tasks
        scheduled_count = 0
        current_time = start_date
        max_retries = 5
        retry_count = 0
        failed_tasks = set()
        task_retry_counts = defaultdict(int)
        max_iterations = total_tasks * 10
        iteration_count = 0

        while (
                ready_tasks or scheduled_count < total_tasks) and retry_count < max_retries and iteration_count < max_iterations:
            iteration_count += 1

            if not ready_tasks and scheduled_count + len(failed_tasks) < total_tasks:
                if not silent_mode:
                    print(
                        f"\n[DEBUG] No ready tasks but {total_tasks - scheduled_count - len(failed_tasks)} tasks remain unscheduled")
                unscheduled = [t for t in all_tasks if t not in self.task_schedule and t not in failed_tasks]

                newly_ready = []
                for task in unscheduled:
                    if task in failed_tasks:
                        continue
                    deps = dependencies.get(task, set())
                    unscheduled_deps = [d for d in deps if d not in self.task_schedule and d not in failed_tasks]
                    if len(unscheduled_deps) == 0:
                        priority = self.calculate_task_priority(task)
                        heapq.heappush(ready_tasks, (priority, task))
                        newly_ready.append(task)

                if newly_ready and not silent_mode:
                    print(f"[DEBUG] Found {len(newly_ready)} newly ready tasks")
                elif not newly_ready and not silent_mode:
                    print(f"\n[ERROR] No more tasks can be scheduled")
                    break

            if not ready_tasks:
                if not silent_mode:
                    print(f"[ERROR] Ready task queue is empty unexpectedly!")
                break

            priority, task_id = heapq.heappop(ready_tasks)

            # Check if this task has failed too many times
            if task_retry_counts[task_id] >= 3:
                if task_id not in failed_tasks:
                    failed_tasks.add(task_id)
                    if not silent_mode:
                        print(f"[WARNING] Task {task_id} failed too many times, skipping permanently")
                continue

            if scheduled_count % 10 == 0 and not silent_mode:
                task_type = self.tasks[task_id]['task_type']
                print(f"\n[DEBUG] Scheduling task {task_id} ({task_type}, priority: {priority:.1f})")

            # Find product line
            product_line = None
            if task_id in self.quality_inspections:
                primary_task = self.quality_inspections[task_id]['primary_task']
                for product, tasks in self.product_tasks.items():
                    if primary_task in tasks or task_id in tasks:
                        product_line = product
                        break
            else:
                for product, tasks in self.product_tasks.items():
                    if task_id in tasks:
                        product_line = product
                        break

            if not product_line:
                if not silent_mode:
                    print(f"[WARNING] No product line found for task {task_id} - skipping")
                continue

            # Get task details
            task_info = self.tasks[task_id]
            duration = task_info['duration']
            mechanics_needed = task_info['mechanics_required']
            is_quality = task_info['is_quality']
            task_type = task_info['task_type']

            # Find earliest available time considering dependencies
            earliest_start = current_time

            # Special handling for late part tasks - respect on-dock date
            if task_id in self.late_part_tasks:
                late_part_earliest = self.get_earliest_start_for_late_part(task_id)
                earliest_start = max(earliest_start, late_part_earliest)
                if scheduled_count % 10 == 0 and not silent_mode:
                    print(f"[DEBUG]   Late part task, earliest start after on-dock: {late_part_earliest}")

            # Check dependency constraints
            constraint_count = 0
            for dep in dependencies.get(task_id, set()):
                if dep in self.task_schedule:
                    dep_end = self.task_schedule[dep]['end_time']
                    constraint_count += 1

                    # Check if this is a Finish = Start relationship
                    is_finish_equals_start = False
                    for constraint in dynamic_constraints:
                        if (constraint['First'] == dep and
                                constraint['Second'] == task_id and
                                constraint['Relationship'] == 'Finish = Start'):
                            is_finish_equals_start = True
                            break

                    if is_finish_equals_start:
                        earliest_start = dep_end
                    else:
                        earliest_start = max(earliest_start, dep_end)

            if scheduled_count % 10 == 0 and constraint_count > 0 and not silent_mode:
                print(f"[DEBUG]   Constrained by {constraint_count} dependencies, earliest start: {earliest_start}")

            # Find next available working time with capacity
            if is_quality:
                # Try to find a quality team with capacity
                scheduled_start = None
                team = None
                shift = None

                for try_shift in ['1st', '2nd', '3rd']:
                    temp_team = self.assign_quality_team_balanced(try_shift, mechanics_needed)
                    if temp_team:
                        try:
                            temp_start, _ = self.get_next_working_time_with_capacity(
                                earliest_start, product_line, temp_team, mechanics_needed,
                                duration, is_quality=True)
                            if not scheduled_start or temp_start < scheduled_start:
                                scheduled_start = temp_start
                                team = temp_team
                                shift = try_shift
                        except:
                            continue

                if not team:
                    task_retry_counts[task_id] += 1
                    if task_retry_counts[task_id] < 3:
                        heapq.heappush(ready_tasks, (priority + 0.1, task_id))
                    else:
                        failed_tasks.add(task_id)
                    continue
            else:
                team = task_info['team']
                try:
                    scheduled_start, shift = self.get_next_working_time_with_capacity(
                        earliest_start, product_line, team, mechanics_needed,
                        duration, is_quality=False)
                except Exception as e:
                    task_retry_counts[task_id] += 1
                    if task_retry_counts[task_id] < 3:
                        heapq.heappush(ready_tasks, (priority + 0.1, task_id))
                    else:
                        failed_tasks.add(task_id)
                    continue

            # Schedule the task
            scheduled_end = scheduled_start + timedelta(minutes=int(duration))

            self.task_schedule[task_id] = {
                'start_time': scheduled_start,
                'end_time': scheduled_end,
                'team': team,
                'product_line': product_line,
                'duration': duration,
                'mechanics_required': mechanics_needed,
                'is_quality': is_quality,
                'task_type': task_type,  # Store task type
                'shift': shift
            }

            scheduled_count += 1
            retry_count = 0

            if scheduled_count % 10 == 0 and not silent_mode:
                print(
                    f"[DEBUG]   Scheduled: {scheduled_start.strftime('%Y-%m-%d %H:%M')} - {scheduled_end.strftime('%H:%M')} ({team}, {shift} shift)")

            # Progress reporting
            if scheduled_count % 25 == 0 and not silent_mode:
                print(
                    f"\n[PROGRESS] {scheduled_count}/{total_tasks} tasks scheduled ({scheduled_count / total_tasks * 100:.1f}%)")

            # Add newly ready tasks
            newly_ready = []
            for dependent in dependents.get(task_id, set()):
                if dependent in self.task_schedule or dependent in failed_tasks:
                    continue
                deps = dependencies.get(dependent, set())
                if all(d in self.task_schedule or d in failed_tasks for d in deps):
                    priority = self.calculate_task_priority(dependent)
                    heapq.heappush(ready_tasks, (priority, dependent))
                    newly_ready.append(dependent)

        if not silent_mode:
            print(f"\n[DEBUG] Scheduling complete! Scheduled {scheduled_count}/{total_tasks} tasks.")

            # Report task type breakdown
            scheduled_by_type = defaultdict(int)
            for task_id in self.task_schedule:
                scheduled_by_type[self.tasks[task_id]['task_type']] += 1

            print("\n[DEBUG] Scheduled tasks by type:")
            for task_type, count in sorted(scheduled_by_type.items()):
                total_of_type = sum(1 for t in self.tasks.values() if t['task_type'] == task_type)
                print(f"  - {task_type}: {count}/{total_of_type}")

        # Restore original debug setting
        self.debug = original_debug

    def validate_dag(self):
        """Validate the DAG for cycles and other issues"""
        print("\nValidating task dependency graph...")

        dynamic_constraints = self.build_dynamic_dependencies()

        # Create adjacency list for cycle detection
        graph = defaultdict(set)
        all_tasks_in_constraints = set()

        for constraint in dynamic_constraints:
            first = constraint['First']
            second = constraint['Second']
            graph[first].add(second)
            all_tasks_in_constraints.add(first)
            all_tasks_in_constraints.add(second)

        # Check if all tasks in constraints exist in task list
        missing_tasks = all_tasks_in_constraints - set(self.tasks.keys())
        if missing_tasks:
            print(f"ERROR: Tasks referenced in constraints but not defined: {missing_tasks}")
            return False

        # Validate product associations for late parts and rework
        print("\nValidating product associations...")
        orphan_late_parts = []
        orphan_rework = []

        for task_id in self.late_part_tasks:
            found_in_product = False
            for product, tasks in self.product_tasks.items():
                if task_id in tasks:
                    found_in_product = True
                    break
            if not found_in_product:
                orphan_late_parts.append(task_id)

        for task_id in self.rework_tasks:
            found_in_product = False
            for product, tasks in self.product_tasks.items():
                if task_id in tasks:
                    found_in_product = True
                    break
            if not found_in_product:
                orphan_rework.append(task_id)

        if orphan_late_parts:
            print(f"WARNING: Late part tasks not associated with any product: {orphan_late_parts}")
        if orphan_rework:
            print(f"WARNING: Rework tasks not associated with any product: {orphan_rework}")

        # Detect cycles using DFS
        def has_cycle_dfs(node, visited, rec_stack, path):
            visited.add(node)
            rec_stack.add(node)
            path.append(node)

            for neighbor in graph.get(node, []):
                if neighbor not in visited:
                    if has_cycle_dfs(neighbor, visited, rec_stack, path):
                        return True
                elif neighbor in rec_stack:
                    # Found a cycle
                    cycle_start = path.index(neighbor)
                    cycle = path[cycle_start:] + [neighbor]
                    print(f"ERROR: Cycle detected: {' -> '.join(map(str, cycle))}")
                    return True

            path.pop()
            rec_stack.remove(node)
            return False

        # Check for cycles
        visited = set()
        for node in all_tasks_in_constraints:
            if node not in visited:
                if has_cycle_dfs(node, visited, set(), []):
                    return False

        # Check for unreachable tasks
        all_tasks = set(self.tasks.keys())
        reachable = set()

        # Find root tasks (no predecessors)
        root_tasks = set()
        for task in all_tasks:
            has_predecessor = False
            for constraint in dynamic_constraints:
                if constraint['Second'] == task:
                    has_predecessor = True
                    break
            if not has_predecessor:
                root_tasks.add(task)

        # BFS from root tasks to find all reachable tasks
        queue = deque(root_tasks)
        reachable.update(root_tasks)

        while queue:
            current = queue.popleft()
            for neighbor in graph.get(current, []):
                if neighbor not in reachable:
                    reachable.add(neighbor)
                    queue.append(neighbor)

        unreachable = all_tasks - reachable
        if unreachable:
            print(f"WARNING: {len(unreachable)} tasks are unreachable from root tasks")

        # Summary statistics with task type breakdown
        print(f"\nDAG Validation Summary:")

        task_type_counts = defaultdict(int)
        for task_id in all_tasks:
            task_type_counts[self.tasks[task_id]['task_type']] += 1

        print(f"- Total tasks: {len(all_tasks)}")
        for task_type, count in sorted(task_type_counts.items()):
            print(f"  • {task_type}: {count}")

        print(f"- Total constraints: {len(dynamic_constraints)}")
        print(f"- Root tasks (no dependencies): {len(root_tasks)}")
        print(f"- Reachable tasks: {len(reachable)}")

        print("\nDAG validation completed successfully!")
        return True

    def is_working_day(self, date, product_line):
        """Check if a date is a working day for a specific product line"""
        if date.weekday() >= 5:  # Saturday = 5, Sunday = 6
            return False

        if date.date() in [h.date() for h in self.holidays[product_line]]:
            return False

        return True

    def check_team_capacity_at_time(self, team, start_time, end_time, mechanics_needed):
        """Check if team has available capacity during the specified time period"""
        capacity = self.team_capacity.get(team, 0) or self.quality_team_capacity.get(team, 0)

        # Get all tasks scheduled for this team
        team_tasks = [(task_id, sched) for task_id, sched in self.task_schedule.items()
                      if sched['team'] == team]

        # Check capacity at every minute in the time range
        current = start_time
        while current < end_time:
            usage = 0
            for task_id, sched in team_tasks:
                if sched['start_time'] <= current < sched['end_time']:
                    usage += sched['mechanics_required']

            if usage + mechanics_needed > capacity:
                return False

            current += timedelta(minutes=1)

        return True

    def get_next_working_time_with_capacity(self, current_time, product_line, team, mechanics_needed, duration,
                                            is_quality=False):
        """Get the next available working time when team has capacity for the task"""
        max_iterations = 5000
        iterations = 0

        while iterations < max_iterations:
            iterations += 1

            # Check if current day is working day
            if not self.is_working_day(current_time, product_line):
                current_time = current_time.replace(hour=6, minute=0, second=0)
                current_time += timedelta(days=1)
                continue

            # Check team shifts
            current_minutes = current_time.hour * 60 + current_time.minute
            available_shift = None

            if is_quality:
                # For quality teams, check quality team shifts
                for shift in ['1st', '2nd', '3rd']:
                    if shift == '1st' and 360 <= current_minutes < 870:
                        if any(t for t, shifts in self.quality_team_shifts.items()
                               if shift in shifts):
                            available_shift = shift
                            break
                    elif shift == '2nd' and 870 <= current_minutes < 1380:
                        if any(t for t, shifts in self.quality_team_shifts.items()
                               if shift in shifts):
                            available_shift = shift
                            break
                    elif shift == '3rd' and (current_minutes >= 1380 or current_minutes < 360):
                        if any(t for t, shifts in self.quality_team_shifts.items()
                               if shift in shifts):
                            available_shift = shift
                            break
            else:
                # Regular mechanic teams
                for shift in self.team_shifts.get(team, []):
                    if shift == '1st' and 360 <= current_minutes < 870:
                        available_shift = shift
                        break
                    elif shift == '2nd' and 870 <= current_minutes < 1380:
                        available_shift = shift
                        break
                    elif shift == '3rd' and (current_minutes >= 1380 or current_minutes < 360):
                        available_shift = shift
                        break

            if available_shift:
                # Check if team has capacity for this task
                end_time = current_time + timedelta(minutes=duration)
                if self.check_team_capacity_at_time(team, current_time, end_time, mechanics_needed):
                    return current_time, available_shift
                else:
                    # Move to next minute and try again
                    current_time += timedelta(minutes=1)
            else:
                # Move to next available shift
                if current_minutes < 360:
                    current_time = current_time.replace(hour=6, minute=0, second=0)
                elif current_minutes < 870:
                    current_time = current_time.replace(hour=14, minute=30, second=0)
                elif current_minutes < 1380:
                    current_time = current_time.replace(hour=23, minute=0, second=0)
                else:
                    current_time = current_time.replace(hour=6, minute=0, second=0)
                    current_time += timedelta(days=1)

        raise RuntimeError(f"[ERROR] Could not find working time with capacity after {max_iterations} iterations!")

    def assign_quality_team_balanced(self, shift, mechanics_needed):
        """Assign quality team with load balancing"""
        available_teams = [team for team, shifts in self.quality_team_shifts.items()
                           if shift in shifts]

        if not available_teams:
            return None

        # Calculate current load for each team
        team_loads = {}
        for team in available_teams:
            # Check if team has capacity
            capacity = self.quality_team_capacity.get(team, 0)
            if capacity < mechanics_needed:
                continue

            scheduled_minutes = sum(
                sched['duration'] * sched['mechanics_required']
                for sched in self.task_schedule.values()
                if sched['team'] == team
            )
            team_loads[team] = scheduled_minutes

        if not team_loads:
            return None

        # Return team with lowest load
        best_team = min(team_loads.items(), key=lambda x: x[1])[0]
        return best_team

    def calculate_critical_path_length(self, task_id):
        """Calculate the length of the critical path from this task to end"""
        if task_id in self._critical_path_cache:
            return self._critical_path_cache[task_id]

        dynamic_constraints = self.build_dynamic_dependencies()

        def get_path_length(task):
            if task in self._critical_path_cache:
                return self._critical_path_cache[task]

            max_successor_path = 0
            task_duration = self.tasks[task]['duration']

            # Find all successors
            for constraint in dynamic_constraints:
                if constraint['First'] == task:
                    successor = constraint['Second']
                    if successor in self.tasks:  # Ensure successor exists
                        successor_path = get_path_length(successor)
                        max_successor_path = max(max_successor_path, successor_path)

            self._critical_path_cache[task] = task_duration + max_successor_path
            return self._critical_path_cache[task]

        return get_path_length(task_id)

    def calculate_task_priority(self, task_id):
        """Enhanced priority calculation with task type and product-specific considerations"""
        # Late part tasks get high priority to avoid blocking downstream work
        if task_id in self.late_part_tasks:
            return -2000

        # Quality inspections get high priority to minimize gaps
        if task_id in self.quality_inspections:
            return -1000

        # Rework tasks get moderately high priority
        if task_id in self.rework_tasks:
            return -500

        # Find product line
        product_line = None

        # Check explicit product associations first
        if task_id in self.task_to_product:
            product_line = self.task_to_product[task_id]
        else:
            # Find which product contains this task
            for product, tasks in self.product_tasks.items():
                if task_id in tasks:
                    product_line = product
                    break

        if not product_line:
            return 999999

        # 1. Delivery date urgency
        delivery_date = self.delivery_dates[product_line]
        days_to_delivery = (delivery_date - datetime.now()).days

        # 2. Critical path length from this task
        critical_path_length = self.calculate_critical_path_length(task_id)

        # 3. Number of direct dependent tasks
        dynamic_constraints = self.build_dynamic_dependencies()
        dependent_count = sum(1 for c in dynamic_constraints if c['First'] == task_id)

        # 4. Task duration
        duration = int(self.tasks[task_id]['duration'])

        # Calculate priority score (lower is higher priority)
        priority = (
                (100 - days_to_delivery) * 10 +  # Urgency factor
                (10000 - critical_path_length) * 5 +  # Critical path factor (inverted)
                (100 - dependent_count) * 3 +  # Dependency factor
                (100 - duration / 10) * 2  # Duration factor
        )

        return priority

    def check_resource_conflicts(self):
        """Enhanced resource conflict detection that tracks usage over time"""
        conflicts = []

        if not self.task_schedule:
            return conflicts

        # Group tasks by team
        team_tasks = defaultdict(list)
        for task_id, schedule in self.task_schedule.items():
            team_tasks[schedule['team']].append((task_id, schedule))

        # Check each team's resource usage
        for team, tasks in team_tasks.items():
            # Get team capacity
            capacity = self.team_capacity.get(team, 0) or self.quality_team_capacity.get(team, 0)

            # Create timeline of resource usage
            events = []
            for task_id, schedule in tasks:
                events.append((schedule['start_time'], schedule['mechanics_required'], 'start', task_id))
                events.append((schedule['end_time'], -schedule['mechanics_required'], 'end', task_id))

            # Sort events by time
            events.sort(key=lambda x: (x[0], x[1]))

            # Track resource usage over time
            current_usage = 0
            for time, delta, event_type, task_id in events:
                if event_type == 'start':
                    current_usage += delta
                    if current_usage > capacity:
                        conflicts.append({
                            'team': team,
                            'time': time,
                            'usage': current_usage,
                            'capacity': capacity,
                            'task': task_id
                        })
                else:
                    current_usage += delta  # delta is negative for 'end'

        return conflicts

    def calculate_slack_time(self, task_id):
        """Calculate slack time for a task based on delivery date"""
        # Find product line
        product_line = None

        # Check explicit product associations first
        if task_id in self.task_to_product:
            product_line = self.task_to_product[task_id]
        elif task_id in self.quality_inspections:
            primary_task = self.quality_inspections[task_id]['primary_task']
            if primary_task in self.task_to_product:
                product_line = self.task_to_product[primary_task]
            else:
                for product, tasks in self.product_tasks.items():
                    if primary_task in tasks:
                        product_line = product
                        break
        else:
            for product, tasks in self.product_tasks.items():
                if task_id in tasks:
                    product_line = product
                    break

        if not product_line:
            return float('inf')

        delivery_date = self.delivery_dates[product_line]

        # Calculate latest start time working backwards from delivery
        latest_finish = delivery_date

        # Get cached dynamic constraints
        dynamic_constraints = self.build_dynamic_dependencies()

        # Get all tasks that must follow this one
        all_successors = set()
        stack = [task_id]

        while stack:
            current = stack.pop()

            for constraint in dynamic_constraints:
                if constraint['First'] == current:
                    successor = constraint['Second']
                    if successor not in all_successors:
                        all_successors.add(successor)
                        stack.append(successor)

        # Calculate total duration of successor chain
        total_successor_duration = sum(int(self.tasks[succ]['duration'])
                                       for succ in all_successors if succ in self.tasks)

        # Add buffer for working hours and days
        buffer_days = total_successor_duration / (8 * 60)  # Assuming 8 hour work days
        latest_start = latest_finish - timedelta(days=buffer_days + 2)  # 2 day safety buffer

        # Return slack in hours
        if task_id in self.task_schedule:
            scheduled_start = self.task_schedule[task_id]['start_time']
            slack = (latest_start - scheduled_start).total_seconds() / 3600
            return slack
        else:
            return 0

    def generate_global_priority_list(self, allow_late_delivery=True, silent_mode=False):
        """Generate the final prioritized task list with task type information"""
        # First schedule all tasks
        self.schedule_tasks(allow_late_delivery=allow_late_delivery, silent_mode=silent_mode)

        # Check for resource conflicts
        conflicts = self.check_resource_conflicts()
        if conflicts and not silent_mode:
            print(f"\n[WARNING] Found {len(conflicts)} resource conflicts:")
            for conflict in conflicts[:5]:  # Show first 5
                print(
                    f"  - {conflict['team']} at {conflict['time']}: {conflict['usage']}/{conflict['capacity']} (task {conflict['task']})")

        # Create priority list based on scheduled start times and slack
        priority_data = []

        for task_id, schedule in self.task_schedule.items():
            slack = self.calculate_slack_time(task_id)

            # Get task type from schedule
            task_type = schedule['task_type']

            # Create display name based on task type
            if task_type == 'Quality Inspection':
                primary_task = self.quality_inspections.get(task_id, {}).get('primary_task', task_id)
                display_name = f"QI for Task {primary_task}"
            elif task_type == 'Late Part':
                # Show which product this late part affects
                product_info = f" ({schedule['product_line']})" if 'product_line' in schedule else ""
                display_name = f"Late Part {task_id}{product_info}"
            elif task_type == 'Rework':
                # Show which product this rework affects
                product_info = f" ({schedule['product_line']})" if 'product_line' in schedule else ""
                display_name = f"Rework {task_id}{product_info}"
            else:
                display_name = f"Task {task_id}"

            priority_data.append({
                'task_id': task_id,
                'task_type': task_type,
                'display_name': display_name,
                'product_line': schedule['product_line'],
                'team': schedule['team'],
                'scheduled_start': schedule['start_time'],
                'scheduled_end': schedule['end_time'],
                'duration_minutes': schedule['duration'],
                'mechanics_required': schedule['mechanics_required'],
                'slack_hours': slack,
                'priority_score': self.calculate_task_priority(task_id),
                'shift': schedule['shift']
            })

        # Sort by scheduled start time, then by slack (less slack = higher priority)
        priority_data.sort(key=lambda x: (x['scheduled_start'], x['slack_hours']))

        # Assign global priority rank
        for i, task in enumerate(priority_data, 1):
            task['global_priority'] = i

        self.global_priority_list = priority_data

        return priority_data

    def filter_by_team(self, team_name):
        """Filter the global priority list for a specific team"""
        return [task for task in self.global_priority_list if task['team'] == team_name]

    def get_daily_schedule(self, date, team_name=None):
        """Get schedule for a specific day, optionally filtered by team"""
        target_date = pd.to_datetime(date).date()

        daily_tasks = []
        for task in self.global_priority_list:
            if task['scheduled_start'].date() == target_date:
                if team_name is None or task['team'] == team_name:
                    daily_tasks.append(task)

        return sorted(daily_tasks, key=lambda x: x['scheduled_start'])

    def calculate_lateness_metrics(self):
        """Calculate lateness metrics for each product line"""
        metrics = {}

        # Check if all tasks were scheduled
        scheduled_count = len(self.task_schedule)
        total_tasks = len(self.tasks)

        for product, delivery_date in self.delivery_dates.items():
            product_tasks = [t for t in self.global_priority_list
                             if t['product_line'] == product]

            if product_tasks:
                last_task_end = max(t['scheduled_end'] for t in product_tasks)
                lateness_days = (last_task_end - delivery_date).days

                # Count task types
                task_type_counts = defaultdict(int)
                for task in product_tasks:
                    task_type_counts[task['task_type']] += 1

                metrics[product] = {
                    'delivery_date': delivery_date,
                    'projected_completion': last_task_end,
                    'lateness_days': lateness_days,
                    'on_time': lateness_days <= 0,
                    'total_tasks': len(product_tasks),
                    'task_breakdown': dict(task_type_counts)
                }
            else:
                # No tasks scheduled for this product
                metrics[product] = {
                    'delivery_date': delivery_date,
                    'projected_completion': None,
                    'lateness_days': 999999,  # Indicate failure
                    'on_time': False,
                    'total_tasks': 0,
                    'task_breakdown': {}
                }

        # Add warning if not all tasks scheduled
        if scheduled_count < total_tasks and not self.debug:
            print(f"\n[WARNING] Lateness metrics based on {scheduled_count}/{total_tasks} scheduled tasks")

        return metrics

    def calculate_makespan(self):
        """Calculate the total makespan (schedule duration) in days"""
        if not self.task_schedule:
            return 0

        # Check if all tasks were scheduled
        scheduled_count = len(self.task_schedule)
        total_tasks = len(self.tasks)
        if scheduled_count < total_tasks:
            # Return a very large number to indicate failure
            return 999999

        start_time = min(sched['start_time'] for sched in self.task_schedule.values())
        end_time = max(sched['end_time'] for sched in self.task_schedule.values())

        # Calculate working days between start and end
        current = start_time.date()
        end_date = end_time.date()
        working_days = 0

        while current <= end_date:
            # Check if it's a working day for any product
            is_working = False
            for product in self.product_tasks.keys():
                if self.is_working_day(datetime.combine(current, datetime.min.time()), product):
                    is_working = True
                    break

            if is_working:
                working_days += 1

            current += timedelta(days=1)

        return working_days

    def export_results(self, filename='scheduling_results.csv', scenario_name=''):
        """Export the global priority list to CSV with enhanced task type information"""
        if scenario_name:
            base = 'scheduling_results'
            ext = 'csv'
            if '.' in filename:
                base, ext = filename.rsplit('.', 1)
            filename = f"{base}_{scenario_name}.{ext}"

        if self.global_priority_list:
            df = pd.DataFrame(self.global_priority_list)
            df.to_csv(filename, index=False)
            print(f"Results exported to {filename}")
        else:
            print(f"[WARNING] No tasks to export to {filename}")

        # Also export lateness metrics
        metrics = self.calculate_lateness_metrics()
        if metrics:
            # Prepare metrics for DataFrame
            metrics_data = []
            for product, data in metrics.items():
                row = {
                    'Product Line': product,
                    'Delivery Date': data['delivery_date'],
                    'Projected Completion': data['projected_completion'],
                    'Lateness Days': data['lateness_days'],
                    'On Time': data['on_time'],
                    'Total Tasks': data['total_tasks']
                }
                # Add task type breakdown
                for task_type, count in data.get('task_breakdown', {}).items():
                    row[f'{task_type} Tasks'] = count
                metrics_data.append(row)

            metrics_df = pd.DataFrame(metrics_data)
            metrics_df.set_index('Product Line', inplace=True)

            if scenario_name:
                metrics_filename = f'lateness_metrics_{scenario_name}.csv'
            else:
                metrics_filename = 'lateness_metrics.csv'

            metrics_df.to_csv(metrics_filename)
            print(f"Lateness metrics exported to {metrics_filename}")
        else:
            print("[WARNING] No lateness metrics to export")

    # ========== SCENARIO 1: Use CSV Headcount ==========
    def scenario_1_csv_headcount(self):
        """
        Scenario 1: Schedule with headcount as defined in CSV, allow late delivery if necessary
        """
        print("\n" + "=" * 80)
        print("SCENARIO 1: Scheduling with CSV-defined Headcount")
        print("=" * 80)

        # Display current capacities from CSV
        print("\nMechanic Team Capacities (from CSV):")
        total_mechanics = 0
        for team, capacity in sorted(self.team_capacity.items()):
            shifts = self.team_shifts.get(team, [])
            total_mechanics += capacity
            print(f"  {team}: {capacity} mechanics (shifts: {', '.join(shifts)})")

        print(f"\nTotal Mechanics: {total_mechanics}")

        print("\nQuality Team Capacities (from CSV):")
        total_quality = 0
        for team, capacity in sorted(self.quality_team_capacity.items()):
            shifts = self.quality_team_shifts.get(team, [])
            total_quality += capacity
            print(f"  {team}: {capacity} quality inspectors (shifts: {', '.join(shifts)})")

        print(f"\nTotal Quality Inspectors: {total_quality}")
        print(f"Total Workforce: {total_mechanics + total_quality}")

        # Generate schedule with allow_late_delivery=True
        priority_list = self.generate_global_priority_list(allow_late_delivery=True)

        # Calculate metrics
        makespan = self.calculate_makespan()
        metrics = self.calculate_lateness_metrics()

        # Display results
        print(f"\nMakespan: {makespan} working days")
        print("\nDelivery Analysis:")
        print("-" * 80)

        total_late_days = 0
        for product, data in metrics.items():
            if data['projected_completion'] is not None:
                status = "ON TIME" if data['on_time'] else f"LATE by {data['lateness_days']} days"
                print(f"{product}: Due {data['delivery_date'].strftime('%Y-%m-%d')}, "
                      f"Projected {data['projected_completion'].strftime('%Y-%m-%d')} - {status}")
            else:
                print(f"{product}: Due {data['delivery_date'].strftime('%Y-%m-%d')}, "
                      f"Projected UNSCHEDULED - FAILED")
            if data['lateness_days'] > 0 and data['lateness_days'] < 999999:
                total_late_days += data['lateness_days']

        print(f"\nTotal lateness across all products: {total_late_days} days")

        # Export results
        self.export_results(scenario_name='scenario1_csv_capacity')

        return {
            'makespan': makespan,
            'metrics': metrics,
            'total_late_days': total_late_days,
            'priority_list': priority_list,
            'team_capacities': dict(self.team_capacity),
            'quality_capacities': dict(self.quality_team_capacity)
        }

    # ========== SCENARIO 1 Alternative: Custom Headcount ==========
    def scenario_1_custom_headcount(self, mechanic_headcount=None, quality_headcount=None,
                                    custom_team_capacity=None, custom_quality_capacity=None):
        """
        Scenario 1 Alternative: Schedule with custom headcount per team

        Args:
            mechanic_headcount: Number of mechanics per mechanic team (applies to all teams)
            quality_headcount: Number of quality inspectors per quality team (applies to all teams)
            custom_team_capacity: Dict of specific team capacities {'Mechanic Team 1': 10, ...}
            custom_quality_capacity: Dict of specific quality team capacities
        """
        print("\n" + "=" * 80)
        print("SCENARIO 1: Custom Headcount Scheduling")
        print("=" * 80)

        # Save original capacities
        original_team = self.team_capacity.copy()
        original_quality = self.quality_team_capacity.copy()

        # Update team capacities
        if custom_team_capacity:
            for team, capacity in custom_team_capacity.items():
                if team in self.team_capacity:
                    self.team_capacity[team] = capacity
        elif mechanic_headcount is not None:
            for team in self.team_capacity:
                self.team_capacity[team] = mechanic_headcount

        if custom_quality_capacity:
            for team, capacity in custom_quality_capacity.items():
                if team in self.quality_team_capacity:
                    self.quality_team_capacity[team] = capacity
        elif quality_headcount is not None:
            for team in self.quality_team_capacity:
                self.quality_team_capacity[team] = quality_headcount

        # Display updated capacities
        print("\nMechanic Team Capacities:")
        for team, capacity in sorted(self.team_capacity.items()):
            print(f"  {team}: {capacity} mechanics")

        print("\nQuality Team Capacities:")
        for team, capacity in sorted(self.quality_team_capacity.items()):
            print(f"  {team}: {capacity} quality inspectors")

        # Generate schedule
        priority_list = self.generate_global_priority_list(allow_late_delivery=True)

        # Calculate metrics
        makespan = self.calculate_makespan()
        metrics = self.calculate_lateness_metrics()

        # Display results
        print(f"\nMakespan: {makespan} working days")
        print("\nDelivery Analysis:")
        print("-" * 80)

        total_late_days = 0
        for product, data in metrics.items():
            if data['projected_completion'] is not None:
                status = "ON TIME" if data['on_time'] else f"LATE by {data['lateness_days']} days"
                print(f"{product}: Due {data['delivery_date'].strftime('%Y-%m-%d')}, "
                      f"Projected {data['projected_completion'].strftime('%Y-%m-%d')} - {status}")
            else:
                print(f"{product}: Due {data['delivery_date'].strftime('%Y-%m-%d')}, "
                      f"Projected UNSCHEDULED - FAILED")
            if data['lateness_days'] > 0 and data['lateness_days'] < 999999:
                total_late_days += data['lateness_days']

        print(f"\nTotal lateness across all products: {total_late_days} days")

        # Export results
        scenario_name = 'scenario1_custom'
        if mechanic_headcount and quality_headcount:
            scenario_name = f'scenario1_{mechanic_headcount}m_{quality_headcount}q'
        self.export_results(scenario_name=scenario_name)

        # Restore original capacities
        self.team_capacity = original_team
        self.quality_team_capacity = original_quality

        return {
            'makespan': makespan,
            'metrics': metrics,
            'total_late_days': total_late_days,
            'priority_list': priority_list
        }

    # ========== SCENARIO 2: Minimize Makespan ==========
    def scenario_2_minimize_makespan(self, min_mechanics=1, max_mechanics=50, min_quality=1, max_quality=20):
        """
        Scenario 2: Find minimum headcount needed to achieve shortest possible makespan

        Args:
            min_mechanics: Minimum mechanics to try per team
            max_mechanics: Maximum mechanics to try per team
            min_quality: Minimum quality inspectors to try per team
            max_quality: Maximum quality inspectors to try per team
        """
        print("\n" + "=" * 80)
        print("SCENARIO 2: Minimize Makespan - Finding Minimum Resources for Shortest Schedule")
        print("=" * 80)

        best_makespan = float('inf')
        best_config = None
        results = []

        # Binary search for mechanics first
        print("\nPhase 1: Finding optimal mechanic headcount...")
        mech_low, mech_high = min_mechanics, max_mechanics
        best_mech = max_mechanics

        while mech_low <= mech_high:
            mech_mid = (mech_low + mech_high) // 2

            # Reset capacities
            for team in self.team_capacity:
                self.team_capacity[team] = mech_mid
            for team in self.quality_team_capacity:
                self.quality_team_capacity[team] = max_quality  # Use max quality for now

            # Clear cache and schedule
            self._critical_path_cache = {}

            try:
                self.generate_global_priority_list(allow_late_delivery=True, silent_mode=True)

                # Check if all tasks were scheduled
                scheduled_count = len(self.task_schedule)
                total_tasks = len(self.tasks)

                if scheduled_count < total_tasks:
                    print(f"  Mechanics: {mech_mid} -> Failed to schedule all tasks ({scheduled_count}/{total_tasks})")
                    mech_low = mech_mid + 1
                    continue

                makespan = self.calculate_makespan()

                print(f"  Mechanics: {mech_mid} -> Makespan: {makespan} days")

                if makespan < best_makespan:
                    best_makespan = makespan
                    best_mech = mech_mid
                    mech_high = mech_mid - 1  # Try lower
                else:
                    mech_low = mech_mid + 1  # Need more

            except Exception as e:
                print(f"  Mechanics: {mech_mid} -> Failed to schedule: {str(e)}")
                mech_low = mech_mid + 1

        print(f"\nOptimal mechanics per team: {best_mech}")

        # Now find optimal quality headcount
        print("\nPhase 2: Finding optimal quality headcount...")
        qual_low, qual_high = min_quality, max_quality
        best_qual = max_quality

        # Set mechanics to optimal
        for team in self.team_capacity:
            self.team_capacity[team] = best_mech

        while qual_low <= qual_high:
            qual_mid = (qual_low + qual_high) // 2

            # Update quality capacity
            for team in self.quality_team_capacity:
                self.quality_team_capacity[team] = qual_mid

            # Clear cache and schedule
            self._critical_path_cache = {}

            try:
                self.generate_global_priority_list(allow_late_delivery=True, silent_mode=True)

                # Check if all tasks were scheduled
                scheduled_count = len(self.task_schedule)
                total_tasks = len(self.tasks)

                if scheduled_count < total_tasks:
                    print(f"  Quality: {qual_mid} -> Failed to schedule all tasks ({scheduled_count}/{total_tasks})")
                    qual_low = qual_mid + 1
                    continue

                makespan = self.calculate_makespan()

                print(f"  Quality: {qual_mid} -> Makespan: {makespan} days")

                if makespan <= best_makespan:  # Accept equal or better
                    best_makespan = makespan
                    best_qual = qual_mid
                    qual_high = qual_mid - 1  # Try lower
                else:
                    qual_low = qual_mid + 1  # Need more

            except Exception as e:
                print(f"  Quality: {qual_mid} -> Failed to schedule: {str(e)}")
                qual_low = qual_mid + 1

        print(f"\nOptimal quality inspectors per team: {best_qual}")

        # Final run with optimal configuration
        print("\nPhase 3: Generating optimal schedule...")
        for team in self.team_capacity:
            self.team_capacity[team] = best_mech
        for team in self.quality_team_capacity:
            self.quality_team_capacity[team] = best_qual

        self.task_schedule = {}
        self._critical_path_cache = {}
        priority_list = self.generate_global_priority_list(allow_late_delivery=True, silent_mode=True)

        makespan = self.calculate_makespan()
        metrics = self.calculate_lateness_metrics()

        # Display final results
        print("\n" + "=" * 80)
        print("OPTIMAL CONFIGURATION FOUND")
        print("=" * 80)
        print(f"Mechanics per team: {best_mech}")
        print(f"Quality inspectors per team: {best_qual}")
        print(f"Minimum makespan: {makespan} working days")

        print("\nDelivery Analysis with Optimal Headcount:")
        print("-" * 80)

        total_late_days = 0
        for product, data in metrics.items():
            if data['projected_completion'] is not None:
                status = "ON TIME" if data['on_time'] else f"LATE by {data['lateness_days']} days"
                print(f"{product}: Due {data['delivery_date'].strftime('%Y-%m-%d')}, "
                      f"Projected {data['projected_completion'].strftime('%Y-%m-%d')} - {status}")
            else:
                print(f"{product}: Due {data['delivery_date'].strftime('%Y-%m-%d')}, "
                      f"Projected UNSCHEDULED - FAILED")
            if data['lateness_days'] > 0 and data['lateness_days'] < 999999:
                total_late_days += data['lateness_days']

        # Calculate total headcount
        total_mechanics = best_mech * len(self.team_capacity)
        total_quality = best_qual * len(self.quality_team_capacity)
        total_headcount = total_mechanics + total_quality

        print(f"\nTotal workforce required:")
        print(f"  - Mechanics: {total_mechanics} ({len(self.team_capacity)} teams × {best_mech})")
        print(f"  - Quality: {total_quality} ({len(self.quality_team_capacity)} teams × {best_qual})")
        print(f"  - TOTAL: {total_headcount}")

        # Export results
        self.export_results(scenario_name=f'scenario2_optimal_{best_mech}m_{best_qual}q')

        # Restore original capacities
        for team, capacity in self._original_team_capacity.items():
            self.team_capacity[team] = capacity
        for team, capacity in self._original_quality_capacity.items():
            self.quality_team_capacity[team] = capacity

        return {
            'optimal_mechanics': best_mech,
            'optimal_quality': best_qual,
            'makespan': makespan,
            'metrics': metrics,
            'total_headcount': total_headcount,
            'priority_list': priority_list
        }

    # ========== Utility methods for optimization scenarios ==========
    def _identify_blocking_teams(self, unscheduled_tasks):
        """Identify which teams are blocking unscheduled tasks"""
        blocking_teams = {'mechanic': set(), 'quality': set()}

        for task_id in unscheduled_tasks:
            if task_id in self.tasks:
                task_info = self.tasks[task_id]
                if task_info['is_quality']:
                    # Find quality teams that could handle this
                    for team in self.quality_team_capacity:
                        blocking_teams['quality'].add(team)
                else:
                    # Add the specific mechanic team
                    blocking_teams['mechanic'].add(task_info['team'])

        return blocking_teams

    def _identify_bottleneck_teams(self):
        """Identify bottleneck teams by analyzing schedule congestion"""
        bottlenecks = {'mechanic': set(), 'quality': set()}

        # Analyze team utilization and queue lengths
        team_load = defaultdict(lambda: {'total_minutes': 0, 'peak_concurrent': 0})

        for task_id, schedule in self.task_schedule.items():
            team = schedule['team']
            duration = schedule['duration']
            mechanics = schedule['mechanics_required']

            team_load[team]['total_minutes'] += duration * mechanics

            # Check concurrent usage at task start
            concurrent = 0
            for other_id, other_schedule in self.task_schedule.items():
                if (other_schedule['team'] == team and
                        other_schedule['start_time'] <= schedule['start_time'] < other_schedule['end_time']):
                    concurrent += other_schedule['mechanics_required']

            team_load[team]['peak_concurrent'] = max(team_load[team]['peak_concurrent'], concurrent)

        # Find teams at or near capacity
        for team, load_data in team_load.items():
            capacity = self.team_capacity.get(team, 0) or self.quality_team_capacity.get(team, 0)
            if load_data['peak_concurrent'] >= capacity * 0.9:
                if team in self.team_capacity:
                    bottlenecks['mechanic'].add(team)
                else:
                    bottlenecks['quality'].add(team)

        return bottlenecks

    def _calculate_team_utilization(self):
        """Calculate detailed utilization metrics for each team"""
        utilization = {'mechanic': {}, 'quality': {}}

        # Working minutes per day per shift
        minutes_per_shift = 8.5 * 60
        total_days = self.calculate_makespan()

        # Calculate for mechanic teams
        for team in self.team_capacity:
            scheduled_minutes = 0
            max_concurrent = 0

            for task_id, schedule in self.task_schedule.items():
                if schedule['team'] == team:
                    scheduled_minutes += schedule['duration'] * schedule['mechanics_required']

                    # Track max concurrent need
                    concurrent_at_start = sum(
                        s['mechanics_required'] for s in self.task_schedule.values()
                        if s['team'] == team and
                        s['start_time'] <= schedule['start_time'] < s['end_time']
                    )
                    max_concurrent = max(max_concurrent, concurrent_at_start)

            capacity = self.team_capacity[team]
            shifts_per_day = len(self.team_shifts.get(team, []))
            available_minutes = capacity * shifts_per_day * minutes_per_shift * total_days

            utilization['mechanic'][team] = {
                'utilization': scheduled_minutes / available_minutes if available_minutes > 0 else 0,
                'scheduled_minutes': scheduled_minutes,
                'available_minutes': available_minutes,
                'max_concurrent': max_concurrent
            }

        # Calculate for quality teams
        for team in self.quality_team_capacity:
            scheduled_minutes = 0
            max_concurrent = 0

            for task_id, schedule in self.task_schedule.items():
                if schedule['team'] == team:
                    scheduled_minutes += schedule['duration'] * schedule['mechanics_required']

                    # Track max concurrent need
                    concurrent_at_start = sum(
                        s['mechanics_required'] for s in self.task_schedule.values()
                        if s['team'] == team and
                        s['start_time'] <= schedule['start_time'] < s['end_time']
                    )
                    max_concurrent = max(max_concurrent, concurrent_at_start)

            capacity = self.quality_team_capacity[team]
            shifts_per_day = len(self.quality_team_shifts.get(team, []))
            available_minutes = capacity * shifts_per_day * minutes_per_shift * total_days

            utilization['quality'][team] = {
                'utilization': scheduled_minutes / available_minutes if available_minutes > 0 else 0,
                'scheduled_minutes': scheduled_minutes,
                'available_minutes': available_minutes,
                'max_concurrent': max_concurrent
            }

        return utilization

    def _test_configuration(self, config):
        """Test if a configuration meets all delivery dates"""
        # Apply configuration
        for team, capacity in config['mechanic'].items():
            self.team_capacity[team] = capacity
        for team, capacity in config['quality'].items():
            self.quality_team_capacity[team] = capacity

        # Clear cache and schedule
        self.task_schedule = {}
        self._critical_path_cache = {}

        try:
            # Generate schedule
            self.generate_global_priority_list(allow_late_delivery=True, silent_mode=True)

            # Check if all tasks scheduled
            if len(self.task_schedule) < len(self.tasks):
                return False

            # Check delivery dates
            metrics = self.calculate_lateness_metrics()
            max_lateness = max((data['lateness_days'] for data in metrics.values()
                                if data['lateness_days'] < 999999), default=0)

            return max_lateness <= 0

        except:
            return False

    # ========== SCENARIO 3: Multi-Dimensional Optimization ==========
    def scenario_3_multidimensional_optimization(self, min_mechanics=1, max_mechanics=20,
                                                 min_quality=1, max_quality=10,
                                                 max_iterations=300):
        """
        Scenario 3 Advanced: Multi-dimensional optimization to find minimum achievable lateness
        and the minimum headcount per team to achieve it.

        This uses a two-phase iterative refinement algorithm:
        Phase 1: Find minimum achievable lateness
        - Starts with minimum capacity
        - Increases bottleneck teams until lateness stops improving
        - Accepts the minimum achievable lateness (may not be zero)

        Phase 2: Optimize workforce while maintaining minimum lateness
        - Reduces capacity for underutilized teams
        - Ensures lateness doesn't increase beyond the minimum found
        """
        print("\n" + "=" * 80)
        print("SCENARIO 3: Multi-Dimensional Team Optimization")
        print("=" * 80)
        print("Finding minimum achievable lateness and optimal capacity per team...")

        # Save original capacities
        original_team = self._original_team_capacity.copy()
        original_quality = self._original_quality_capacity.copy()

        # Initialize with minimum capacities
        current_mech_config = {team: min_mechanics for team in original_team}
        current_qual_config = {team: min_quality for team in original_quality}

        # Track best configuration found
        best_config = None
        best_total_workforce = float('inf')
        best_metrics = None
        best_max_lateness = float('inf')
        best_total_lateness = float('inf')

        # Track if we're still improving
        iterations_without_improvement = 0
        max_iterations_without_improvement = 20

        # Phase 1: Find minimum achievable lateness
        print("\nPhase 1: Finding minimum achievable lateness...")
        iteration = 0
        phase1_complete = False
        previous_max_lateness = float('inf')
        previous_total_lateness = float('inf')

        while iteration < max_iterations and not phase1_complete:
            iteration += 1

            # Apply current configuration
            for team, capacity in current_mech_config.items():
                self.team_capacity[team] = capacity
            for team, capacity in current_qual_config.items():
                self.quality_team_capacity[team] = capacity

            # Clear cache and schedule
            self.task_schedule = {}
            self._critical_path_cache = {}

            try:
                # Generate schedule
                self.generate_global_priority_list(allow_late_delivery=True, silent_mode=True)

                # Check if all tasks scheduled
                scheduled_count = len(self.task_schedule)
                total_tasks = len(self.tasks)

                if scheduled_count < total_tasks:
                    # Find which team types are blocking
                    unscheduled_tasks = [t for t in self.tasks if t not in self.task_schedule]
                    blocking_teams = self._identify_blocking_teams(unscheduled_tasks)

                    # Increase capacity for blocking teams
                    capacity_increased = False
                    for team in blocking_teams['mechanic']:
                        if current_mech_config[team] < max_mechanics:
                            current_mech_config[team] += 1
                            capacity_increased = True
                            if iteration % 10 == 1:
                                print(
                                    f"  Iteration {iteration}: Increased {team} to {current_mech_config[team]} mechanics")

                    for team in blocking_teams['quality']:
                        if current_qual_config[team] < max_quality:
                            current_qual_config[team] += 1
                            capacity_increased = True
                            if iteration % 10 == 1:
                                print(
                                    f"  Iteration {iteration}: Increased {team} to {current_qual_config[team]} quality")

                    if not capacity_increased:
                        print(f"\n[WARNING] Cannot increase capacity further. Max limits reached.")
                        print(f"[INFO] Accepting current lateness as minimum achievable.")
                        phase1_complete = True
                    continue

                # Calculate metrics
                metrics = self.calculate_lateness_metrics()
                max_lateness = max((data['lateness_days'] for data in metrics.values()
                                    if data['lateness_days'] < 999999), default=0)
                total_lateness = sum(max(0, data['lateness_days']) for data in metrics.values()
                                     if data['lateness_days'] < 999999)

                # Calculate total workforce
                total_workforce = (sum(current_mech_config.values()) +
                                   sum(current_qual_config.values()))

                # Check if we've improved
                improved = False
                if max_lateness < previous_max_lateness:
                    improved = True
                    previous_max_lateness = max_lateness
                elif max_lateness == previous_max_lateness and total_lateness < previous_total_lateness:
                    improved = True
                    previous_total_lateness = total_lateness

                if iteration % 10 == 1 or improved or max_lateness == 0:
                    print(f"  Iteration {iteration}: Max lateness = {max_lateness} days, "
                          f"Total lateness = {total_lateness} days, "
                          f"Workforce = {total_workforce}")

                # Save if this is the best so far
                if max_lateness < best_max_lateness or (
                        max_lateness == best_max_lateness and total_lateness < best_total_lateness):
                    best_max_lateness = max_lateness
                    best_total_lateness = total_lateness
                    best_config = {
                        'mechanic': current_mech_config.copy(),
                        'quality': current_qual_config.copy()
                    }
                    best_total_workforce = total_workforce
                    best_metrics = metrics
                    iterations_without_improvement = 0

                    if max_lateness == 0:
                        print(f"\n✓ Achieved zero lateness at iteration {iteration}!")
                        phase1_complete = True
                else:
                    iterations_without_improvement += 1

                # Check if we should stop (no improvement for many iterations)
                if iterations_without_improvement >= max_iterations_without_improvement:
                    print(f"\n[INFO] No improvement for {max_iterations_without_improvement} iterations.")
                    print(f"[INFO] Minimum achievable lateness: {best_max_lateness} days")
                    phase1_complete = True
                    continue

                # If not improving, identify bottlenecks and increase their capacity
                if not improved:
                    bottlenecks = self._identify_bottleneck_teams()

                    # Focus on teams causing the most lateness
                    capacity_increased = False

                    # Prioritize mechanic teams first
                    for team in bottlenecks['mechanic']:
                        if current_mech_config[team] < max_mechanics:
                            current_mech_config[team] += 2  # Increase by 2 for faster convergence
                            capacity_increased = True
                            break

                    if not capacity_increased:
                        for team in bottlenecks['quality']:
                            if current_qual_config[team] < max_quality:
                                current_qual_config[team] += 1
                                capacity_increased = True
                                break

                    # If no bottlenecks identified, increase the team with minimum capacity
                    if not capacity_increased:
                        min_mech_cap = min(current_mech_config.values())
                        for team, cap in current_mech_config.items():
                            if cap == min_mech_cap and cap < max_mechanics:
                                current_mech_config[team] += 1
                                capacity_increased = True
                                break

                    if not capacity_increased:
                        min_qual_cap = min(current_qual_config.values())
                        for team, cap in current_qual_config.items():
                            if cap == min_qual_cap and cap < max_quality:
                                current_qual_config[team] += 1
                                capacity_increased = True
                                break

                    if not capacity_increased:
                        print(f"\n[INFO] All teams at maximum capacity.")
                        print(f"[INFO] Minimum achievable lateness: {best_max_lateness} days")
                        phase1_complete = True

            except Exception as e:
                print(f"  Iteration {iteration}: Scheduling failed - {str(e)}")
                # Increase minimum capacity teams
                min_mech = min(current_mech_config.values())
                for team in current_mech_config:
                    if current_mech_config[team] == min_mech and current_mech_config[team] < max_mechanics:
                        current_mech_config[team] += 1
                        break

        if best_config is None:
            print("\n[ERROR] Could not find any feasible solution!")
            # Restore and return
            for team, capacity in original_team.items():
                self.team_capacity[team] = capacity
            for team, capacity in original_quality.items():
                self.quality_team_capacity[team] = capacity
            return None

        print(f"\n✓ Phase 1 Complete!")
        print(f"  Minimum achievable max lateness: {best_max_lateness} days")
        print(f"  Total lateness: {best_total_lateness} days")
        print(f"  Initial workforce: {best_total_workforce}")

        # Phase 2: Optimize by reducing underutilized teams while maintaining minimum lateness
        print("\nPhase 2: Optimizing workforce while maintaining minimum lateness...")

        target_max_lateness = best_max_lateness
        target_total_lateness = best_total_lateness * 1.1  # Allow 10% increase in total for optimization

        improved = True
        optimization_iterations = 0

        while improved and optimization_iterations < 50:
            improved = False
            optimization_iterations += 1

            # Calculate team utilization with current configuration
            for team, capacity in best_config['mechanic'].items():
                self.team_capacity[team] = capacity
            for team, capacity in best_config['quality'].items():
                self.quality_team_capacity[team] = capacity

            # Generate schedule to analyze utilization
            self.task_schedule = {}
            self._critical_path_cache = {}
            self.generate_global_priority_list(allow_late_delivery=True, silent_mode=True)

            # Calculate utilization for each team
            team_utilization = self._calculate_team_utilization()

            # Try reducing capacity for underutilized mechanic teams
            for team, util_data in sorted(team_utilization['mechanic'].items(),
                                          key=lambda x: x[1]['utilization']):
                if util_data['utilization'] < 0.7 and best_config['mechanic'][team] > min_mechanics:
                    # Try reducing by 1
                    test_config = {
                        'mechanic': best_config['mechanic'].copy(),
                        'quality': best_config['quality'].copy()
                    }
                    test_config['mechanic'][team] -= 1

                    # Test if still maintains minimum lateness
                    if self._test_configuration_with_lateness_target(test_config, target_max_lateness,
                                                                     target_total_lateness):
                        best_config = test_config
                        best_total_workforce -= 1
                        improved = True
                        print(f"  Reduced {team} to {test_config['mechanic'][team]} "
                              f"(utilization was {util_data['utilization']:.1%})")
                        break  # One change at a time

            # Try reducing quality teams if no mechanic reduction worked
            if not improved:
                for team, util_data in sorted(team_utilization['quality'].items(),
                                              key=lambda x: x[1]['utilization']):
                    if util_data['utilization'] < 0.7 and best_config['quality'][team] > min_quality:
                        # Check if this team handles multi-person inspections
                        max_inspectors_needed = util_data.get('max_concurrent', 1)
                        if best_config['quality'][team] > max_inspectors_needed:
                            # Try reducing by 1
                            test_config = {
                                'mechanic': best_config['mechanic'].copy(),
                                'quality': best_config['quality'].copy()
                            }
                            test_config['quality'][team] -= 1

                            # Test if still maintains minimum lateness
                            if self._test_configuration_with_lateness_target(test_config, target_max_lateness,
                                                                             target_total_lateness):
                                best_config = test_config
                                best_total_workforce -= 1
                                improved = True
                                print(f"  Reduced {team} to {test_config['quality'][team]} "
                                      f"(utilization was {util_data['utilization']:.1%})")
                                break

        # Phase 3: Final verification and results
        print("\nPhase 3: Final verification...")

        # Apply best configuration
        for team, capacity in best_config['mechanic'].items():
            self.team_capacity[team] = capacity
        for team, capacity in best_config['quality'].items():
            self.quality_team_capacity[team] = capacity

        # Generate final schedule
        self.task_schedule = {}
        self._critical_path_cache = {}
        priority_list = self.generate_global_priority_list(allow_late_delivery=True, silent_mode=True)

        # Calculate final metrics
        makespan = self.calculate_makespan()
        metrics = self.calculate_lateness_metrics()

        final_max_lateness = max((data['lateness_days'] for data in metrics.values()
                                  if data['lateness_days'] < 999999), default=0)
        final_total_lateness = sum(max(0, data['lateness_days']) for data in metrics.values()
                                   if data['lateness_days'] < 999999)

        # Display results
        print("\n" + "=" * 80)
        print("MULTI-DIMENSIONAL OPTIMIZATION RESULTS")
        print("=" * 80)

        print(f"\nMinimum Achievable Lateness:")
        print(f"  Maximum lateness: {final_max_lateness} days")
        print(f"  Total lateness: {final_total_lateness} days")

        print("\nOptimized Mechanic Team Capacities:")
        total_mechanics = 0
        for team in sorted(best_config['mechanic']):
            capacity = best_config['mechanic'][team]
            original = original_team[team]
            total_mechanics += capacity
            diff = capacity - original
            symbol = "↑" if diff > 0 else "↓" if diff < 0 else "="
            print(f"  {team}: {capacity} mechanics (was {original}, {symbol}{abs(diff)})")

        print(f"\nOptimized Quality Team Capacities:")
        total_quality = 0
        for team in sorted(best_config['quality']):
            capacity = best_config['quality'][team]
            original = original_quality[team]
            total_quality += capacity
            diff = capacity - original
            symbol = "↑" if diff > 0 else "↓" if diff < 0 else "="
            print(f"  {team}: {capacity} inspectors (was {original}, {symbol}{abs(diff)})")

        print(f"\nWorkforce Summary:")
        print(f"  Total Mechanics: {total_mechanics} (was {sum(original_team.values())})")
        print(f"  Total Quality: {total_quality} (was {sum(original_quality.values())})")
        print(f"  TOTAL WORKFORCE: {total_mechanics + total_quality} "
              f"(was {sum(original_team.values()) + sum(original_quality.values())})")

        original_total = sum(original_team.values()) + sum(original_quality.values())
        new_total = total_mechanics + total_quality

        if new_total < original_total:
            savings = original_total - new_total
            print(f"  SAVINGS: {savings} workers ({(savings / original_total * 100):.1f}% reduction)")
        elif new_total > original_total:
            increase = new_total - original_total
            print(f"  INCREASE: {increase} workers ({(increase / original_total * 100):.1f}% more)")

        print(f"\nSchedule Metrics:")
        print(f"  Makespan: {makespan} working days")

        print("\nDelivery Status by Product:")
        for product in sorted(metrics.keys()):
            data = metrics[product]
            if data['projected_completion'] is not None:
                if data['on_time']:
                    status = "✓ ON TIME"
                    days_info = f"({(data['delivery_date'] - data['projected_completion']).days} days early)"
                else:
                    status = "✗ LATE"
                    days_info = f"({data['lateness_days']} days late)"
                print(f"  {product}: {status} {days_info}")
                print(f"    Due: {data['delivery_date'].strftime('%Y-%m-%d')}, "
                      f"Projected: {data['projected_completion'].strftime('%Y-%m-%d')}")
            else:
                print(f"  {product}: ✗ UNSCHEDULED")

        # Export results
        self.export_results(scenario_name='scenario3_minimum_lateness_optimized')

        # Restore original capacities
        for team, capacity in original_team.items():
            self.team_capacity[team] = capacity
        for team, capacity in original_quality.items():
            self.quality_team_capacity[team] = capacity

        return {
            'config': best_config,
            'total_workforce': total_mechanics + total_quality,
            'makespan': makespan,
            'metrics': metrics,
            'max_lateness': final_max_lateness,
            'total_lateness': final_total_lateness,
            'priority_list': priority_list
        }

    def _test_configuration(self, config):
        """Test if a configuration meets all delivery dates"""
        # Apply configuration
        for team, capacity in config['mechanic'].items():
            self.team_capacity[team] = capacity
        for team, capacity in config['quality'].items():
            self.quality_team_capacity[team] = capacity

        # Clear cache and schedule
        self.task_schedule = {}
        self._critical_path_cache = {}

        try:
            # Generate schedule
            self.generate_global_priority_list(allow_late_delivery=True, silent_mode=True)

            # Check if all tasks scheduled
            if len(self.task_schedule) < len(self.tasks):
                return False

            # Check delivery dates
            metrics = self.calculate_lateness_metrics()
            max_lateness = max((data['lateness_days'] for data in metrics.values()
                                if data['lateness_days'] < 999999), default=0)

            return max_lateness <= 0

        except:
            return False

    def _test_configuration_with_lateness_target(self, config, target_max_lateness, target_total_lateness):
        """Test if a configuration maintains the target lateness levels"""
        # Apply configuration
        for team, capacity in config['mechanic'].items():
            self.team_capacity[team] = capacity
        for team, capacity in config['quality'].items():
            self.quality_team_capacity[team] = capacity

        # Clear cache and schedule
        self.task_schedule = {}
        self._critical_path_cache = {}

        try:
            # Generate schedule
            self.generate_global_priority_list(allow_late_delivery=True, silent_mode=True)

            # Check if all tasks scheduled
            if len(self.task_schedule) < len(self.tasks):
                return False

            # Check lateness metrics
            metrics = self.calculate_lateness_metrics()
            max_lateness = max((data['lateness_days'] for data in metrics.values()
                                if data['lateness_days'] < 999999), default=0)
            total_lateness = sum(max(0, data['lateness_days']) for data in metrics.values()
                                 if data['lateness_days'] < 999999)

            # Must not exceed target max lateness and should be close to target total
            return max_lateness <= target_max_lateness and total_lateness <= target_total_lateness

        except:
            return False


# ========== MAIN EXECUTION ==========
if __name__ == "__main__":
    try:
        # Instantiate the scheduler
        scheduler = ProductionScheduler('scheduling_data.csv', debug=True)
        scheduler.load_data_from_csv()

        # Run scheduling
        baseline_list = scheduler.generate_global_priority_list()

        # Diagnostic print for unscheduled tasks
        unscheduled = [t for t in scheduler.tasks if t not in scheduler.task_schedule]
        print(f"\nUnscheduled tasks ({len(unscheduled)}):")
        for t in unscheduled:
            info = scheduler.tasks[t]
            product = scheduler.task_to_product.get(t, "None")
            team = info.get('team', "None")
            print(f"  Task {t}: type={info['task_type']}, team={team}, product={product}")

            # Print dependencies for this task
            dynamic_constraints = scheduler.build_dynamic_dependencies()
            deps = [c['First'] for c in dynamic_constraints if c['Second'] == t]
            print(f"    Dependencies: {deps}")
            unsched_deps = [d for d in deps if d in unscheduled]
            if unsched_deps:
                print(f"    UNSCHEDULED dependencies: {unsched_deps}")

        # Load data
        print("Loading data from CSV...")
        scheduler.load_data_from_csv()

        # Display summary of loaded data
        print("\n" + "=" * 80)
        print("DATA LOADED SUCCESSFULLY")
        print("=" * 80)

        # Count task types
        task_type_counts = defaultdict(int)
        for task_info in scheduler.tasks.values():
            task_type_counts[task_info['task_type']] += 1

        print(f"Total tasks: {len(scheduler.tasks)}")
        for task_type, count in sorted(task_type_counts.items()):
            print(f"- {task_type}: {count}")

        print(f"\nProduct lines: {len(scheduler.delivery_dates)}")
        print(f"Mechanic teams: {len(scheduler.team_capacity)}")
        print(f"Quality teams: {len(scheduler.quality_team_capacity)}")
        print(f"Late part delay: {scheduler.late_part_delay_days} days after on-dock date")

        # Store results
        results = {}

        # BASELINE: Run with original CSV capacities
        print("\n" + "=" * 80)
        print("Running BASELINE scenario with original CSV capacities...")
        print("=" * 80)
        baseline_list = scheduler.generate_global_priority_list()
        results['baseline'] = {
            'makespan': scheduler.calculate_makespan(),
            'metrics': scheduler.calculate_lateness_metrics(),
            'total_workforce': sum(scheduler._original_team_capacity.values()) + sum(
                scheduler._original_quality_capacity.values()),
            'priority_list': baseline_list
        }
        scheduler.export_results(scenario_name='baseline')

        # Display sample of tasks by type
        print("\nSample of scheduled tasks by type:")
        type_samples = defaultdict(list)
        for task in baseline_list[:50]:  # Look at first 50 tasks
            type_samples[task['task_type']].append(task)

        for task_type in ['Late Part', 'Rework', 'Production', 'Quality Inspection']:
            if task_type in type_samples and type_samples[task_type]:
                sample = type_samples[task_type][0]
                print(f"  {task_type}: {sample['display_name']} scheduled at "
                      f"{sample['scheduled_start'].strftime('%Y-%m-%d %H:%M')} "
                      f"for {sample['product_line']}")

        # SCENARIO 1: Use CSV-defined headcount
        print("\n" + "=" * 80)
        print("Running Scenario 1: CSV-defined Capacities...")
        print("=" * 80)
        results['scenario1'] = scheduler.scenario_1_csv_headcount()

        # SCENARIO 2: Find optimal headcount for minimum makespan
        print("\n" + "=" * 80)
        print("Running Scenario 2: Finding Optimal Headcount for Minimum Makespan...")
        print("=" * 80)
        results['scenario2'] = scheduler.scenario_2_minimize_makespan(
            min_mechanics=1, max_mechanics=30,
            min_quality=1, max_quality=10
        )

        # SCENARIO 3: Multi-dimensional optimization
        print("\n" + "=" * 80)
        print("Running Scenario 3: Multi-Dimensional Optimization...")
        print("=" * 80)
        results['scenario3'] = scheduler.scenario_3_multidimensional_optimization(
            min_mechanics=1, max_mechanics=30,  # Increased from 20
            min_quality=1, max_quality=15,  # Increased from 10
            max_iterations=300  # More iterations for convergence
        )

        # ========== FINAL SUMMARY ==========
        print("\n" + "=" * 80)
        print("FINAL RESULTS SUMMARY - ALL SCENARIOS")
        print("=" * 80)

        # Baseline Results
        print("\nBASELINE (Original CSV Capacity):")
        print(f"  Makespan: {results['baseline']['makespan']} days")
        print(f"  Total Workforce: {results['baseline']['total_workforce']}")

        # Show task type breakdown
        baseline_metrics = results['baseline']['metrics']
        if baseline_metrics:
            task_type_totals = defaultdict(int)
            for product_data in baseline_metrics.values():
                for task_type, count in product_data.get('task_breakdown', {}).items():
                    task_type_totals[task_type] += count

            print("  Task Type Breakdown:")
            for task_type, count in sorted(task_type_totals.items()):
                print(f"    - {task_type}: {count}")

        # Scenario 1 Results
        print("\nSCENARIO 1 (CSV Capacity, Allow Late):")
        print(f"  Makespan: {results['scenario1']['makespan']} days")
        print(f"  Total Late Days: {results['scenario1']['total_late_days']}")

        # Scenario 2 Results
        print("\nSCENARIO 2 (Minimize Makespan - Uniform Capacity):")
        print(f"  Makespan: {results['scenario2']['makespan']} days")
        print(f"  Uniform Capacity: {results['scenario2']['optimal_mechanics']} mechanics, "
              f"{results['scenario2']['optimal_quality']} quality per team")
        print(f"  Total Workforce: {results['scenario2']['total_headcount']}")

        # Scenario 3 Results
        if results.get('scenario3'):
            print("\nSCENARIO 3 (Multi-Dimensional Optimization - Minimum Lateness):")
            print(f"  Makespan: {results['scenario3']['makespan']} days")
            print(f"  Maximum Lateness: {results['scenario3']['max_lateness']} days")
            print(f"  Total Lateness: {results['scenario3']['total_lateness']} days")
            print(f"  Total Workforce: {results['scenario3']['total_workforce']}")
            print("  Team-Specific Capacities:")

            # Show mechanic teams
            print("    Mechanic Teams:")
            for team in sorted(results['scenario3']['config']['mechanic']):
                capacity = results['scenario3']['config']['mechanic'][team]
                original = scheduler._original_team_capacity[team]
                diff = capacity - original
                print(f"      {team}: {capacity} (was {original}, {'+' if diff > 0 else ''}{diff})")

            # Show quality teams
            print("    Quality Teams:")
            for team in sorted(results['scenario3']['config']['quality']):
                capacity = results['scenario3']['config']['quality'][team]
                original = scheduler._original_quality_capacity[team]
                diff = capacity - original
                print(f"      {team}: {capacity} (was {original}, {'+' if diff > 0 else ''}{diff})")
        else:
            print("\nSCENARIO 3: Failed to find a solution within constraints")

        print("\nTop 30 Prioritized Tasks (showing task types):")
        print("-" * 100)
        print(f"{'Priority':>8} | {'Task ID':>7} | {'Type':>15} | {'Team':>20} | {'Product':>12} | {'Start':>16}")
        print("-" * 100)

        top_tasks = results['baseline']['priority_list'][:30]
        for task in top_tasks:
            print(f"{task['global_priority']:>8} | "
                  f"{task['task_id']:>7} | "
                  f"{task['task_type'][:15]:>15} | "
                  f"{task['team'][:20]:>20} | "
                  f"{task['product_line'][:12]:>12} | "
                  f"{task['scheduled_start'].strftime('%Y-%m-%d %H:%M'):>16}")
        print("-" * 100)

        # Efficiency Comparison
        print("\n" + "-" * 80)
        print("EFFICIENCY COMPARISON:")
        csv_total = results['baseline']['total_workforce']
        print(f"  CSV Total Workforce: {csv_total}")
        print(f"  Scenario 1: Same as CSV")
        print(f"  Scenario 2: {results['scenario2']['total_headcount']} "
              f"({'+' if results['scenario2']['total_headcount'] > csv_total else ''}"
              f"{results['scenario2']['total_headcount'] - csv_total} vs CSV)")

        if results.get('scenario3'):
            s3_total = results['scenario3']['total_workforce']
            workforce_diff = s3_total - csv_total
            if workforce_diff < 0:
                print(f"  Scenario 3: {s3_total} "
                      f"({round((1 - s3_total / csv_total) * 100)}% reduction vs CSV)")
            elif workforce_diff > 0:
                print(f"  Scenario 3: {s3_total} "
                      f"(+{workforce_diff} vs CSV, {round((workforce_diff / csv_total) * 100)}% increase)")
            else:
                print(f"  Scenario 3: {s3_total} (same as CSV)")

            print(f"\n  Scenario 3 Achievement:")
            if results['scenario3']['max_lateness'] == 0:
                print(f"    → Zero lateness achieved!")
            else:
                print(f"    → Minimum lateness: {results['scenario3']['max_lateness']} days")
                print(f"    → Total lateness: {results['scenario3']['total_lateness']} days")

            # Compare with Scenario 2
            s2_total = results['scenario2']['total_headcount']
            improvement = s2_total - s3_total
            if improvement > 0:
                print(f"\n  Multi-Dimensional Advantage: {improvement} fewer workers than uniform optimization")
                print(f"  ({round(improvement / s2_total * 100, 1)}% improvement over Scenario 2)")
            elif improvement < 0:
                print(f"\n  Note: Scenario 3 uses {-improvement} more workers than Scenario 2")
                print(f"  This is needed to achieve minimum lateness with team-specific constraints")

        print("\n" + "-" * 80)
        print("KEY INSIGHTS:")
        print(f"  ✓ All scenarios process Late Parts, Rework, and Production tasks")
        print(f"  ✓ CSV configuration uses {csv_total} total workers")

        if results.get('scenario3'):
            if results['scenario3']['max_lateness'] == 0:
                print(f"  ✓ Scenario 3 achieved zero lateness with {results['scenario3']['total_workforce']} workers")
            else:
                print(f"  ✓ Minimum achievable lateness: {results['scenario3']['max_lateness']} days")
                print(f"  ✓ Scenario 3 minimized lateness with {results['scenario3']['total_workforce']} workers")

            workforce_diff = results['scenario3']['total_workforce'] - csv_total
            if workforce_diff < 0:
                print(f"  ✓ This saves {-workforce_diff} workers "
                      f"({round(-workforce_diff / csv_total * 100, 1)}%) vs CSV")
            elif workforce_diff > 0:
                print(f"  ✓ This requires {workforce_diff} more workers "
                      f"({round(workforce_diff / csv_total * 100, 1)}%) to achieve minimum lateness")

            print(f"  ✓ Team-specific optimization provides targeted resource allocation")

        print("\n" + "-" * 80)
        print("RECOMMENDATIONS:")
        if results.get('scenario3'):
            if results['scenario3']['max_lateness'] == 0:
                print("  → Scenario 3 achieved on-time delivery for all products!")
                print("  → Use the team-specific capacities shown above for implementation")
            else:
                print(f"  → Scenario 3 found minimum achievable lateness: {results['scenario3']['max_lateness']} days")
                print("  → This is the best possible outcome with the given constraints")
                print("  → Consider:")
                print("    • Accepting this lateness level as optimal")
                print("    • Negotiating delivery dates for late products")
                print("    • Adding overtime or temporary workers for critical periods")
            print("  → Team-specific optimization ensures efficient resource allocation")
            print("  → Monitor actual performance and adjust as needed")

        print("\n" + "=" * 80)
        print("ALL SCENARIOS COMPLETED SUCCESSFULLY!")
        print("=" * 80)

        print("\nOutput Files Generated:")
        print("- scheduling_results_baseline.csv")
        print("- scheduling_results_scenario1_csv_capacity.csv")
        print(f"- scheduling_results_scenario2_optimal_{results['scenario2']['optimal_mechanics']}m_"
              f"{results['scenario2']['optimal_quality']}q.csv")
        if results.get('scenario3'):
            print("- scheduling_results_scenario3_minimum_lateness_optimized.csv")
        print("- Plus corresponding lateness_metrics files for each scenario")

    except Exception as e:
        print("\n" + "!" * 80)
        print(f"ERROR: {str(e)}")
        print("!" * 80)
        import traceback

        traceback.print_exc()