# app.py - Flask Web Server for Production Scheduling Dashboard
# Compatible with Enhanced ProductionScheduler with Product-Specific Tasks

from flask import Flask, render_template, jsonify, request, send_file
from flask_cors import CORS
import pandas as pd
import json
from datetime import datetime, timedelta
import os
from collections import defaultdict
import traceback

# Import your enhanced scheduler
from scheduler import ProductionScheduler

app = Flask(__name__)
CORS(app)  # Enable CORS for API calls

# Global scheduler instance
scheduler = None
scenario_results = {}

def initialize_scheduler():
    """Initialize the scheduler and run all scenarios"""
    global scheduler, scenario_results
    
    try:
        print("=" * 80)
        print("Initializing Production Scheduler Dashboard")
        print("=" * 80)
        
        # Initialize scheduler with enhanced features
        scheduler = ProductionScheduler('scheduling_data.csv', debug=False, late_part_delay_days=1.0)
        scheduler.load_data_from_csv()
        
        print("\nScheduler loaded successfully!")
        print(f"Total tasks: {len(scheduler.tasks)}")
        print(f"Products: {len(scheduler.delivery_dates)}")
        print(f"Late parts: {len(scheduler.late_part_tasks)}")
        print(f"Rework tasks: {len(scheduler.rework_tasks)}")
        
        # Run baseline scenario
        print("\n" + "-" * 40)
        print("Running BASELINE scenario...")
        scheduler.generate_global_priority_list(allow_late_delivery=True, silent_mode=True)
        scenario_results['baseline'] = export_scenario_data(scheduler, 'baseline')
        print(f"✓ Baseline complete: {scenario_results['baseline']['makespan']} days makespan")
        
        # Run Scenario 1
        print("\nRunning SCENARIO 1 (CSV Capacity)...")
        result1 = scheduler.scenario_1_csv_headcount()
        scenario_results['scenario1'] = export_scenario_data(scheduler, 'scenario1', result1)
        print(f"✓ Scenario 1 complete: {scenario_results['scenario1']['makespan']} days makespan")
        
        # Run Scenario 2
        print("\nRunning SCENARIO 2 (Minimize Makespan)...")
        result2 = scheduler.scenario_2_minimize_makespan(
            min_mechanics=1, max_mechanics=30,
            min_quality=1, max_quality=10
        )
        scenario_results['scenario2'] = export_scenario_data(scheduler, 'scenario2', result2)
        print(f"✓ Scenario 2 complete: {scenario_results['scenario2']['makespan']} days makespan")
        
        # Run Scenario 3 - Enhanced Multi-dimensional with minimum lateness
        print("\nRunning SCENARIO 3 (Multi-Dimensional Optimization)...")
        result3 = scheduler.scenario_3_multidimensional_optimization(
            min_mechanics=1, max_mechanics=30,
            min_quality=1, max_quality=15,
            max_iterations=200  # Reduced for faster dashboard loading
        )
        if result3:
            scenario_results['scenario3'] = export_scenario_data(scheduler, 'scenario3', result3)
            print(f"✓ Scenario 3 complete: {scenario_results['scenario3']['makespan']} days makespan")
            if 'maxLateness' in scenario_results['scenario3']:
                print(f"  Maximum lateness: {scenario_results['scenario3']['maxLateness']} days")
        else:
            print("✗ Scenario 3 failed to find solution")
            scenario_results['scenario3'] = create_failed_scenario_data()
        
        print("\n" + "=" * 80)
        print("All scenarios completed successfully!")
        print("=" * 80)
        
        return scenario_results
        
    except Exception as e:
        print(f"\n✗ ERROR during initialization: {str(e)}")
        traceback.print_exc()
        raise


def export_scenario_data(scheduler, scenario_name, result=None):
    """Export scenario data in format needed by dashboard with enhanced product-specific info"""

    # Get metrics
    metrics = scheduler.calculate_lateness_metrics()
    makespan = scheduler.calculate_makespan()

    # Calculate utilization for each team
    utilization_data = calculate_team_utilization(scheduler)

    # Format tasks for dashboard with enhanced product associations
    tasks = []
    for task_data in scheduler.global_priority_list[:500]:  # Export top 500 tasks
        # Get product-specific info for late parts and rework
        product_info = ""
        if task_data['task_id'] in scheduler.task_to_product:
            product_specific = scheduler.task_to_product[task_data['task_id']]
            if task_data['task_type'] in ['Late Part', 'Rework']:
                product_info = f" ({product_specific})"

        # Check for dependencies (late part and rework constraints)
        dependencies = []
        for lp_constraint in scheduler.late_part_constraints:
            if lp_constraint['Second'] == task_data['task_id']:
                dependencies.append({
                    'type': 'Late Part',
                    'task': lp_constraint['First'],
                    'product': lp_constraint.get('Product_Line', 'Unknown')
                })

        for rw_constraint in scheduler.rework_constraints:
            if rw_constraint['Second'] == task_data['task_id']:
                dependencies.append({
                    'type': 'Rework',
                    'task': rw_constraint['First'],
                    'product': rw_constraint.get('Product_Line', 'Unknown')
                })

        tasks.append({
            'priority': task_data['global_priority'],
            'taskId': task_data['task_id'],
            'type': task_data['task_type'],
            'displayName': task_data['display_name'] + product_info,
            'product': task_data['product_line'],
            'team': task_data['team'],
            'startTime': task_data['scheduled_start'].isoformat(),
            'endTime': task_data['scheduled_end'].isoformat(),
            'duration': task_data['duration_minutes'],
            'mechanics': task_data['mechanics_required'],
            'shift': task_data['shift'],
            'slackHours': round(task_data['slack_hours'], 1),
            'dependencies': dependencies,
            'isLatePartTask': task_data['task_id'] in scheduler.late_part_tasks,
            'isReworkTask': task_data['task_id'] in scheduler.rework_tasks,
            'onDockDate': scheduler.on_dock_dates.get(task_data['task_id'], '').isoformat()
            if task_data['task_id'] in scheduler.on_dock_dates else None
        })

    # Format products for dashboard with enhanced metrics
    products = []
    for product_name, delivery_date in scheduler.delivery_dates.items():
        product_metrics = metrics.get(product_name, {})

        # Get product-specific tasks
        product_tasks = [t for t in scheduler.global_priority_list
                         if t['product_line'] == product_name]

        # Count task types for this product
        task_type_counts = defaultdict(int)
        for task in product_tasks:
            task_type_counts[task['task_type']] += 1

        # Count late parts and rework specifically for this product
        late_parts_count = sum(1 for task in product_tasks if task['task_id'] in scheduler.late_part_tasks)
        rework_count = sum(1 for task in product_tasks if task['task_id'] in scheduler.rework_tasks)

        # Calculate progress
        if product_tasks:
            total_duration = sum(t['duration_minutes'] for t in product_tasks)
            # Simple progress estimate based on schedule
            first_task_start = min(t['scheduled_start'] for t in product_tasks)
            last_task_end = max(t['scheduled_end'] for t in product_tasks)
            total_span = (last_task_end - first_task_start).total_seconds() / 60

            # Estimate progress based on current time
            now = datetime.now()
            if now < first_task_start:
                progress = 0
            elif now > last_task_end:
                progress = 100
            else:
                elapsed = (now - first_task_start).total_seconds() / 60
                progress = min(100, int((elapsed / total_span) * 100)) if total_span > 0 else 0
        else:
            progress = 0

        products.append({
            'name': product_name,
            'deliveryDate': delivery_date.isoformat(),
            'onTime': product_metrics.get('on_time', False),
            'latenessDays': product_metrics.get('lateness_days', 0),
            'totalTasks': product_metrics.get('total_tasks', 0),
            'progress': progress,
            'daysRemaining': (delivery_date - datetime.now()).days,
            'criticalPath': len([t for t in product_tasks if t['slack_hours'] < 24]),
            'latePartsCount': late_parts_count,
            'reworkCount': rework_count
        })

    # Calculate team capacities
    team_capacities = {}
    for team, capacity in scheduler.team_capacity.items():
        team_capacities[team] = capacity
    for team, capacity in scheduler.quality_team_capacity.items():
        team_capacities[team] = capacity

    # Calculate summary metrics
    on_time_products = sum(1 for p in products if p['onTime'])
    on_time_rate = int((on_time_products / len(products) * 100)) if products else 0

    avg_utilization = int(sum(utilization_data.values()) / len(utilization_data)) if utilization_data else 0

    total_workforce = sum(scheduler.team_capacity.values()) + sum(scheduler.quality_team_capacity.values())

    # Count task types
    task_type_summary = defaultdict(int)
    for task in tasks:
        task_type_summary[task['type']] += 1

    # Get lateness metrics
    max_lateness = max((m['lateness_days'] for m in metrics.values()
                        if m['lateness_days'] < 999999), default=0)
    total_lateness = sum(max(0, m['lateness_days']) for m in metrics.values()
                         if m['lateness_days'] < 999999)

    # Return complete scenario data
    return {
        'scenarioName': scenario_name,
        'totalWorkforce': total_workforce,
        'makespan': makespan,
        'onTimeRate': on_time_rate,
        'avgUtilization': avg_utilization,
        'maxLateness': max_lateness,
        'totalLateness': total_lateness,
        'teamCapacities': team_capacities,
        'tasks': tasks,
        'products': products,
        'utilization': utilization_data,
        'totalTasks': len(scheduler.tasks),
        'scheduledTasks': len(scheduler.task_schedule),
        'taskTypeSummary': dict(task_type_summary),
        'achievedMaxLateness': result.get('max_lateness') if result else max_lateness
    }

def create_failed_scenario_data():
    """Create placeholder data for failed scenarios"""
    return {
        'scenarioName': 'scenario3',
        'totalWorkforce': 0,
        'makespan': 999999,
        'onTimeRate': 0,
        'avgUtilization': 0,
        'maxLateness': 999999,
        'totalLateness': 999999,
        'teamCapacities': {},
        'tasks': [],
        'products': [],
        'utilization': {},
        'totalTasks': 0,
        'scheduledTasks': 0,
        'error': 'Failed to find solution within constraints'
    }

def calculate_team_utilization(scheduler):
    """Calculate utilization percentage for each team"""
    utilization = {}
    
    if not scheduler.task_schedule:
        return utilization
    
    # Working minutes per shift
    minutes_per_shift = 8.5 * 60
    makespan_days = scheduler.calculate_makespan()
    
    if makespan_days == 0 or makespan_days >= 999999:
        return utilization
    
    # Calculate for mechanic teams
    for team, capacity in scheduler.team_capacity.items():
        scheduled_minutes = 0
        task_count = 0
        
        for task_id, schedule in scheduler.task_schedule.items():
            if schedule['team'] == team:
                scheduled_minutes += schedule['duration'] * schedule['mechanics_required']
                task_count += 1
        
        shifts_per_day = len(scheduler.team_shifts.get(team, []))
        available_minutes = capacity * shifts_per_day * minutes_per_shift * makespan_days
        
        if available_minutes > 0:
            util_percent = min(100, int((scheduled_minutes / available_minutes) * 100))
            utilization[team] = util_percent
        else:
            utilization[team] = 0
    
    # Calculate for quality teams
    for team, capacity in scheduler.quality_team_capacity.items():
        scheduled_minutes = 0
        task_count = 0
        
        for task_id, schedule in scheduler.task_schedule.items():
            if schedule['team'] == team:
                scheduled_minutes += schedule['duration'] * schedule['mechanics_required']
                task_count += 1
        
        shifts_per_day = len(scheduler.quality_team_shifts.get(team, []))
        available_minutes = capacity * shifts_per_day * minutes_per_shift * makespan_days
        
        if available_minutes > 0:
            util_percent = min(100, int((scheduled_minutes / available_minutes) * 100))
            utilization[team] = util_percent
        else:
            utilization[team] = 0
    
    return utilization

# Flask Routes

@app.route('/')
def index():
    """Serve the main dashboard page"""
    return render_template('dashboard.html')

@app.route('/api/scenarios')
def get_scenarios():
    """Get list of available scenarios with descriptions"""
    return jsonify({
        'scenarios': [
            {
                'id': 'baseline', 
                'name': 'Baseline (CSV Capacity)',
                'description': 'Original capacity from CSV file'
            },
            {
                'id': 'scenario1', 
                'name': 'Scenario 1: CSV Headcount',
                'description': 'Schedule with CSV-defined headcount, allow late delivery'
            },
            {
                'id': 'scenario2', 
                'name': 'Scenario 2: Minimize Makespan',
                'description': 'Find uniform headcount for shortest schedule'
            },
            {
                'id': 'scenario3', 
                'name': 'Scenario 3: Multi-Dimensional',
                'description': 'Optimize per-team capacity for minimum lateness'
            }
        ]
    })

@app.route('/api/scenario/<scenario_id>')
def get_scenario_data(scenario_id):
    """Get data for a specific scenario"""
    if scenario_id in scenario_results:
        return jsonify(scenario_results[scenario_id])
    else:
        return jsonify({'error': 'Scenario not found'}), 404

@app.route('/api/scenario/<scenario_id>/summary')
def get_scenario_summary(scenario_id):
    """Get summary statistics for a scenario"""
    if scenario_id not in scenario_results:
        return jsonify({'error': 'Scenario not found'}), 404
    
    data = scenario_results[scenario_id]
    
    # Calculate product-specific summaries
    product_summaries = []
    for product in data.get('products', []):
        product_summaries.append({
            'name': product['name'],
            'status': 'On Time' if product['onTime'] else f"Late by {product['latenessDays']} days",
            'latePartsCount': product.get('latePartsCount', 0),
            'reworkCount': product.get('reworkCount', 0),
            'totalTasks': product['totalTasks']
        })
    
    summary = {
        'scenarioName': data['scenarioName'],
        'totalWorkforce': data['totalWorkforce'],
        'makespan': data['makespan'],
        'onTimeRate': data['onTimeRate'],
        'avgUtilization': data['avgUtilization'],
        'maxLateness': data.get('maxLateness', 0),
        'totalLateness': data.get('totalLateness', 0),
        'achievedMaxLateness': data.get('achievedMaxLateness', data.get('maxLateness', 0)),
        'totalTasks': data['totalTasks'],
        'scheduledTasks': data['scheduledTasks'],
        'taskTypeSummary': data.get('taskTypeSummary', {}),
        'productSummaries': product_summaries
    }
    
    return jsonify(summary)

@app.route('/api/team/<team_name>/tasks')
def get_team_tasks(team_name):
    """Get tasks for a specific team with product-specific info"""
    scenario = request.args.get('scenario', 'baseline')
    shift = request.args.get('shift', 'all')
    limit = int(request.args.get('limit', 30))
    start_date = request.args.get('date', None)
    
    if scenario not in scenario_results:
        return jsonify({'error': 'Scenario not found'}), 404
    
    tasks = scenario_results[scenario]['tasks']
    
    # Filter by team
    if team_name != 'all':
        tasks = [t for t in tasks if t['team'] == team_name]
    
    # Filter by shift
    if shift != 'all':
        tasks = [t for t in tasks if t['shift'] == shift]
    
    # Filter by date if provided
    if start_date:
        target_date = datetime.fromisoformat(start_date).date()
        tasks = [t for t in tasks 
                if datetime.fromisoformat(t['startTime']).date() == target_date]
    
    # Sort by start time and limit
    tasks.sort(key=lambda x: x['startTime'])
    tasks = tasks[:limit]
    
    # Add team capacity info
    team_capacity = scenario_results[scenario]['teamCapacities'].get(team_name, 0)
    team_shifts = []
    if team_name in scheduler.team_shifts:
        team_shifts = scheduler.team_shifts[team_name]
    elif team_name in scheduler.quality_team_shifts:
        team_shifts = scheduler.quality_team_shifts[team_name]
    
    return jsonify({
        'tasks': tasks,
        'total': len(tasks),
        'teamCapacity': team_capacity,
        'teamShifts': team_shifts,
        'utilization': scenario_results[scenario]['utilization'].get(team_name, 0)
    })

@app.route('/api/product/<product_name>/tasks')
def get_product_tasks(product_name):
    """Get all tasks for a specific product including late parts and rework"""
    scenario = request.args.get('scenario', 'baseline')
    
    if scenario not in scenario_results:
        return jsonify({'error': 'Scenario not found'}), 404
    
    tasks = scenario_results[scenario]['tasks']
    
    # Filter by product
    product_tasks = [t for t in tasks if t['product'] == product_name]
    
    # Separate by task type
    task_breakdown = defaultdict(list)
    for task in product_tasks:
        task_breakdown[task['type']].append(task)
    
    # Sort each type by start time
    for task_type in task_breakdown:
        task_breakdown[task_type].sort(key=lambda x: x['startTime'])
    
    # Get product info
    product_info = next((p for p in scenario_results[scenario]['products'] 
                         if p['name'] == product_name), None)
    
    return jsonify({
        'productName': product_name,
        'productInfo': product_info,
        'tasks': product_tasks,
        'taskBreakdown': {k: len(v) for k, v in task_breakdown.items()},
        'tasksByType': dict(task_breakdown),
        'totalTasks': len(product_tasks)
    })

@app.route('/api/mechanic/<mechanic_id>/tasks')
def get_mechanic_tasks(mechanic_id):
    """Get tasks assigned to a specific mechanic"""
    scenario = request.args.get('scenario', 'baseline')
    date = request.args.get('date', datetime.now().isoformat())
    
    if scenario not in scenario_results:
        return jsonify({'error': 'Scenario not found'}), 404
    
    # For demo purposes, assign tasks based on mechanic ID pattern
    # In production, this would query actual assignments from database
    tasks = scenario_results[scenario]['tasks']
    
    # Simple assignment logic for demo
    mechanic_num = int(''.join(filter(str.isdigit, mechanic_id))) if any(c.isdigit() for c in mechanic_id) else 1
    assigned_tasks = []
    
    # Filter tasks by date
    target_date = datetime.fromisoformat(date).date()
    daily_tasks = [t for t in tasks if datetime.fromisoformat(t['startTime']).date() == target_date]
    
    # Assign every Nth task to this mechanic (simple demo logic)
    for i, task in enumerate(daily_tasks):
        if i % 8 == (mechanic_num - 1):  # Distribute among 8 mechanics
            assigned_tasks.append(task)
            if len(assigned_tasks) >= 6:  # Max 6 tasks per day
                break
    
    # Sort by start time
    assigned_tasks.sort(key=lambda x: x['startTime'])
    
    return jsonify({
        'mechanicId': mechanic_id,
        'tasks': assigned_tasks,
        'shift': '1st',  # Would be determined by actual assignment
        'date': date,
        'totalAssigned': len(assigned_tasks)
    })

@app.route('/api/export/<scenario_id>')
def export_scenario(scenario_id):
    """Export scenario data to CSV"""
    if scenario_id not in scenario_results:
        return jsonify({'error': 'Scenario not found'}), 404
    
    data = scenario_results[scenario_id]
    
    # Create DataFrame from tasks
    df = pd.DataFrame(data['tasks'])
    
    # Add additional columns
    df['Scenario'] = scenario_id
    df['MaxLateness'] = data.get('maxLateness', 0)
    df['TotalLateness'] = data.get('totalLateness', 0)
    
    # Save to CSV
    filename = f'export_{scenario_id}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    filepath = os.path.join('exports', filename)
    
    # Create exports directory if it doesn't exist
    os.makedirs('exports', exist_ok=True)
    
    df.to_csv(filepath, index=False)
    
    return send_file(filepath, as_attachment=True, download_name=filename)

@app.route('/api/assign_task', methods=['POST'])
def assign_task():
    """Assign a task to a mechanic"""
    data = request.json
    task_id = data.get('taskId')
    mechanic_id = data.get('mechanicId')
    scenario = data.get('scenario', 'baseline')
    
    # In production, this would update a database
    # For now, just return success
    return jsonify({
        'success': True,
        'taskId': task_id,
        'mechanicId': mechanic_id,
        'message': f'Task {task_id} assigned to {mechanic_id}'
    })

@app.route('/api/refresh', methods=['POST'])
def refresh_data():
    """Refresh all scenario data"""
    try:
        initialize_scheduler()
        return jsonify({
            'success': True,
            'message': 'All scenarios refreshed',
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/teams')
def get_teams():
    """Get list of all teams with their capacities"""
    teams = []
    
    if scheduler:
        # Add mechanic teams
        for team in scheduler.team_capacity:
            teams.append({
                'id': team,
                'type': 'mechanic',
                'capacity': scheduler.team_capacity[team],
                'shifts': scheduler.team_shifts.get(team, [])
            })
        
        # Add quality teams
        for team in scheduler.quality_team_capacity:
            teams.append({
                'id': team,
                'type': 'quality',
                'capacity': scheduler.quality_team_capacity[team],
                'shifts': scheduler.quality_team_shifts.get(team, [])
            })
    
    return jsonify({'teams': teams})

@app.route('/api/mechanics')
def get_mechanics():
    """Get list of all mechanics"""
    # In production, this would come from a database
    mechanics = [
        {'id': 'mech1', 'name': 'John Smith', 'team': 'Mechanic Team 1'},
        {'id': 'mech2', 'name': 'Jane Doe', 'team': 'Mechanic Team 1'},
        {'id': 'mech3', 'name': 'Bob Johnson', 'team': 'Mechanic Team 2'},
        {'id': 'mech4', 'name': 'Alice Williams', 'team': 'Mechanic Team 2'},
        {'id': 'mech5', 'name': 'Charlie Brown', 'team': 'Mechanic Team 3'},
        {'id': 'mech6', 'name': 'Diana Prince', 'team': 'Mechanic Team 3'},
        {'id': 'mech7', 'name': 'Frank Castle', 'team': 'Mechanic Team 4'},
        {'id': 'mech8', 'name': 'Grace Lee', 'team': 'Mechanic Team 4'},
        {'id': 'qual1', 'name': 'Tom Wilson', 'team': 'Quality Team 1'},
        {'id': 'qual2', 'name': 'Sarah Connor', 'team': 'Quality Team 2'},
        {'id': 'qual3', 'name': 'Mike Ross', 'team': 'Quality Team 3'}
    ]
    return jsonify({'mechanics': mechanics})

@app.route('/api/stats')
def get_statistics():
    """Get overall statistics across all scenarios"""
    stats = {
        'scenarios': {},
        'comparison': {}
    }
    
    for scenario_id, data in scenario_results.items():
        stats['scenarios'][scenario_id] = {
            'workforce': data['totalWorkforce'],
            'makespan': data['makespan'],
            'onTimeRate': data['onTimeRate'],
            'utilization': data['avgUtilization'],
            'maxLateness': data.get('maxLateness', 0),
            'totalLateness': data.get('totalLateness', 0)
        }
    
    # Calculate comparisons
    if 'baseline' in scenario_results:
        baseline_workforce = scenario_results['baseline']['totalWorkforce']
        baseline_makespan = scenario_results['baseline']['makespan']
        
        for scenario_id, data in scenario_results.items():
            if scenario_id != 'baseline':
                workforce_diff = data['totalWorkforce'] - baseline_workforce
                makespan_diff = data['makespan'] - baseline_makespan
                
                stats['comparison'][scenario_id] = {
                    'workforceDiff': workforce_diff,
                    'workforcePercent': round((workforce_diff / baseline_workforce) * 100, 1) if baseline_workforce > 0 else 0,
                    'makespanDiff': makespan_diff,
                    'makespanPercent': round((makespan_diff / baseline_makespan) * 100, 1) if baseline_makespan > 0 else 0
                }
    
    return jsonify(stats)

@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'scheduler_loaded': scheduler is not None,
        'scenarios_loaded': len(scenario_results),
        'timestamp': datetime.now().isoformat()
    })

# Error handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500

if __name__ == '__main__':
    try:
        # Initialize scheduler on startup
        print("\nStarting Production Scheduling Dashboard Server...")
        print("-" * 80)
        initialize_scheduler()
        
        print("\n" + "=" * 80)
        print("Server ready! Open your browser to: http://localhost:5000")
        print("=" * 80 + "\n")
        
        # Run Flask app
        app.run(debug=True, host='0.0.0.0', port=5000)
        
    except Exception as e:
        print(f"\n✗ Failed to start server: {str(e)}")
        traceback.print_exc()