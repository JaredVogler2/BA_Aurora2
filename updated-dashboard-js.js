// dashboard.js - Enhanced Client-side JavaScript for Production Scheduling Dashboard
// Compatible with product-specific late parts and rework tasks

let currentScenario = 'baseline';
let currentView = 'team-lead';
let selectedTeam = 'all';
let selectedShift = 'all';
let selectedProduct = 'all';
let scenarioData = {};
let allScenarios = {};

// Initialize dashboard on page load
document.addEventListener('DOMContentLoaded', function() {
    console.log('Initializing Production Scheduling Dashboard...');
    loadAllScenarios();
    setupEventListeners();
    setupProductFilter();
    setupRefreshButton();
});

// Load all scenarios at startup for quick switching
async function loadAllScenarios() {
    try {
        showLoading('Loading scenario data...');
        const scenariosResponse = await fetch('/api/scenarios');
        const scenariosInfo = await scenariosResponse.json();

        for (const scenario of scenariosInfo.scenarios) {
            const response = await fetch(`/api/scenario/${scenario.id}`);
            const data = await response.json();
            if (response.ok) {
                allScenarios[scenario.id] = data;
                console.log(`✓ Loaded ${scenario.id}: ${data.totalTasks} tasks, ${data.makespan} days makespan`);
            } else {
                console.error(`✗ Failed to load ${scenario.id}:`, data.error);
            }
        }

        scenarioData = allScenarios[currentScenario];
        hideLoading();
        updateView();
    } catch (error) {
        console.error('Error loading scenarios:', error);
        showError('Failed to load scenario data. Please refresh the page.');
    }
}

// Setup all event listeners
function setupEventListeners() {
    document.querySelectorAll('.view-tab').forEach(tab => {
        tab.addEventListener('click', function() {
            switchView(this.dataset.view);
        });
    });

    const scenarioSelect = document.getElementById('scenarioSelect');
    if (scenarioSelect) {
        scenarioSelect.addEventListener('change', function() {
            switchScenario(this.value);
        });
    }

    const teamSelect = document.getElementById('teamSelect');
    if (teamSelect) {
        teamSelect.addEventListener('change', function() {
            selectedTeam = this.value;
            updateTeamLeadView();
        });
    }

    const shiftSelect = document.getElementById('shiftSelect');
    if (shiftSelect) {
        shiftSelect.addEventListener('change', function() {
            selectedShift = this.value;
            updateTeamLeadView();
        });
    }

    const mechanicSelect = document.getElementById('mechanicSelect');
    if (mechanicSelect) {
        mechanicSelect.addEventListener('change', function() {
            updateMechanicView();
        });
    }
}

// Setup product filter (new feature)
function setupProductFilter() {
    const teamFilters = document.querySelector('.team-filters');
    if (teamFilters && !document.getElementById('productSelect')) {
        const productFilter = document.createElement('div');
        productFilter.className = 'filter-group';
        productFilter.innerHTML = `
            <label>Product:</label>
            <select id="productSelect">
                <option value="all">All Products</option>
            </select>
        `;
        teamFilters.appendChild(productFilter);

        document.getElementById('productSelect').addEventListener('change', function() {
            selectedProduct = this.value;
            updateTeamLeadView();
        });
    }
}

// Switch scenario with enhanced handling
function switchScenario(scenario) {
    if (allScenarios[scenario]) {
        currentScenario = scenario;
        scenarioData = allScenarios[scenario];
        updateProductFilter();
        showScenarioInfo();
        updateView();
    }
}

// Update product filter dropdown
function updateProductFilter() {
    const productSelect = document.getElementById('productSelect');
    if (productSelect && scenarioData.products) {
        const currentSelection = productSelect.value;
        productSelect.innerHTML = '<option value="all">All Products</option>';
        scenarioData.products.forEach(product => {
            const option = document.createElement('option');
            option.value = product.name;
            option.textContent = `${product.name} (${product.totalTasks} tasks)`;
            productSelect.appendChild(option);
        });
        if ([...productSelect.options].some(opt => opt.value === currentSelection)) {
            productSelect.value = currentSelection;
        } else {
            productSelect.value = 'all';
            selectedProduct = 'all';
        }
    }
}

// Show scenario-specific information
function showScenarioInfo() {
    let infoBanner = document.getElementById('scenarioInfo');
    if (!infoBanner) {
        const mainContent = document.querySelector('.main-content');
        infoBanner = document.createElement('div');
        infoBanner.id = 'scenarioInfo';
        infoBanner.style.cssText = 'background: #f0f9ff; border: 1px solid #3b82f6; border-radius: 8px; padding: 12px; margin-bottom: 20px;';
        mainContent.insertBefore(infoBanner, mainContent.firstChild);
    }

    let infoHTML = `<strong>${currentScenario.toUpperCase()}</strong>: `;
    if (currentScenario === 'scenario3' && scenarioData.achievedMaxLateness !== undefined) {
        if (scenarioData.achievedMaxLateness === 0) {
            infoHTML += `✓ Achieved zero lateness with ${scenarioData.totalWorkforce} workers`;
        } else {
            infoHTML += `Minimum achievable lateness: ${scenarioData.achievedMaxLateness} days (${scenarioData.totalWorkforce} workers)`;
        }
    } else if (currentScenario === 'scenario2') {
        infoHTML += `Optimal uniform capacity: ${scenarioData.optimalMechanics || 'N/A'} mechanics, ${scenarioData.optimalQuality || 'N/A'} quality per team`;
    } else {
        infoHTML += `Workforce: ${scenarioData.totalWorkforce}, Makespan: ${scenarioData.makespan} days`;
    }
    infoBanner.innerHTML = infoHTML;
}

// Switch between views
function switchView(view) {
    document.querySelectorAll('.view-tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.view-content').forEach(v => v.classList.remove('active'));
    document.querySelector(`[data-view="${view}"]`).classList.add('active');
    document.getElementById(`${view}-view`).classList.add('active');
    currentView = view;
    updateView();
}

// Update view based on current selection
function updateView() {
    if (!scenarioData) return;
    if (currentView === 'team-lead') {
        updateTeamLeadView();
    } else if (currentView === 'management') {
        updateManagementView();
    } else if (currentView === 'mechanic') {
        updateMechanicView();
    } else if (currentView === 'project') {
        setupGanttProductFilter();
        setupGanttTeamFilter();
        renderGanttChart();
    }
}

// Enhanced Team Lead View with product-specific filtering
async function updateTeamLeadView() {
    if (!scenarioData) return;

    const teamCap = selectedTeam === 'all' ?
        Object.values(scenarioData.teamCapacities).reduce((a, b) => a + b, 0) :
        scenarioData.teamCapacities[selectedTeam] || 0;
    document.getElementById('teamCapacity').textContent = teamCap;

    let tasks = scenarioData.tasks.filter(task => {
        const teamMatch = selectedTeam === 'all' || task.team === selectedTeam;
        const shiftMatch = selectedShift === 'all' || task.shift === selectedShift;
        const productMatch = selectedProduct === 'all' || task.product === selectedProduct;
        return teamMatch && shiftMatch && productMatch;
    });

    const taskTypeCounts = {};
    tasks.forEach(task => {
        taskTypeCounts[task.type] = (taskTypeCounts[task.type] || 0) + 1;
    });

    const today = new Date();
    const todayTasks = tasks.filter(t => {
        const taskDate = new Date(t.startTime);
        return taskDate.toDateString() === today.toDateString();
    });
    document.getElementById('tasksToday').textContent = todayTasks.length;

    const latePartTasks = tasks.filter(t => t.isLatePartTask).length;
    const reworkTasks = tasks.filter(t => t.isReworkTask).length;

    const util = selectedTeam === 'all' ?
        scenarioData.avgUtilization :
        (scenarioData.utilization[selectedTeam] || 0);
    document.getElementById('teamUtilization').textContent = util + '%';

    const critical = tasks.filter(t =>
        t.priority <= 10 || t.isLatePartTask || t.isReworkTask
    ).length;
    document.getElementById('criticalTasks').textContent = critical;

    const tbody = document.getElementById('taskTableBody');
    tbody.innerHTML = '';

    tasks.sort((a, b) => new Date(a.startTime) - new Date(b.startTime));
    tasks.slice(0, 30).forEach(task => {
        const row = tbody.insertRow();
        const startTime = new Date(task.startTime);

        let typeIndicator = '';
        if (task.isLatePartTask) {
            typeIndicator = ' 📦';
        } else if (task.isReworkTask) {
            typeIndicator = ' 🔧';
        }

        let dependencyInfo = '';
        if (task.dependencies && task.dependencies.length > 0) {
            const deps = task.dependencies.map(d => `${d.type} ${d.task}`).join(', ');
            dependencyInfo = `<span style="color: #6b7280; font-size: 11px;">Deps: ${deps}</span>`;
        }

        row.innerHTML = `
            <td class="priority">${task.priority}</td>
            <td class="task-id">${task.taskId}${typeIndicator}</td>
            <td><span class="task-type ${getTaskTypeClass(task.type)}">${task.type}</span></td>
            <td>${task.product}<br>${dependencyInfo}</td>
            <td>${formatTime(startTime)}</td>
            <td>${task.duration} min</td>
            <td>${task.mechanics}</td>
            <td>
                <select class="assign-select" data-task-id="${task.taskId}">
                    <option value="">Unassigned</option>
                    <option value="mech1">John Smith</option>
                    <option value="mech2">Jane Doe</option>
                    <option value="mech3">Bob Johnson</option>
                    <option value="mech4">Alice Williams</option>
                </select>
            </td>
        `;

        if (task.isLatePartTask) {
            row.style.backgroundColor = '#fef3c7';
        } else if (task.isReworkTask) {
            row.style.backgroundColor = '#fee2e2';
        }
    });

    updateTaskTypeSummary(taskTypeCounts, latePartTasks, reworkTasks);
}

// Update task type summary (new feature)
function updateTaskTypeSummary(taskTypeCounts, latePartCount, reworkCount) {
    let summaryDiv = document.getElementById('taskTypeSummary');
    if (!summaryDiv) {
        const statsContainer = document.querySelector('.team-stats');
        if (statsContainer) {
            summaryDiv = document.createElement('div');
            summaryDiv.id = 'taskTypeSummary';
            summaryDiv.className = 'stat-card';
            summaryDiv.style.gridColumn = 'span 2';
            statsContainer.appendChild(summaryDiv);
        }
    }

    if (summaryDiv) {
        let summaryHTML = '<h3>Task Type Breakdown</h3><div style="display: flex; gap: 15px; margin-top: 10px;">';
        for (const [type, count] of Object.entries(taskTypeCounts)) {
            const color = getTaskTypeColor(type);
            summaryHTML += `
                <div style="flex: 1;">
                    <div style="font-size: 18px; font-weight: bold; color: ${color};">${count}</div>
                    <div style="font-size: 11px; color: #6b7280;">${type}</div>
                </div>
            `;
        }
        summaryHTML += '</div>';
        if (latePartCount > 0 || reworkCount > 0) {
            summaryHTML += '<div style="margin-top: 10px; padding-top: 10px; border-top: 1px solid #e5e7eb;">';
            summaryHTML += `<span style="margin-right: 15px;">📦 Late Parts: ${latePartCount}</span>`;
            summaryHTML += `<span>🔧 Rework: ${reworkCount}</span>`;
            summaryHTML += '</div>';
        }
        summaryDiv.innerHTML = summaryHTML;
    }
}

// Enhanced Management View with lateness metrics
function updateManagementView() {
    if (!scenarioData) return;
    document.getElementById('totalWorkforce').textContent = scenarioData.totalWorkforce;
    document.getElementById('makespan').textContent = scenarioData.makespan;
    document.getElementById('onTimeRate').textContent = scenarioData.onTimeRate + '%';
    document.getElementById('avgUtilization').textContent = scenarioData.avgUtilization + '%';

    let latenessCard = document.getElementById('latenessMetrics');
    if (!latenessCard) {
        const metricsGrid = document.querySelector('.metrics-grid');
        if (metricsGrid) {
            latenessCard = document.createElement('div');
            latenessCard.className = 'metric-card';
            latenessCard.id = 'latenessMetrics';
            metricsGrid.appendChild(latenessCard);
        }
    }

    if (latenessCard) {
        let latenessHTML = '<h3>Lateness Metrics</h3>';
        if (scenarioData.achievedMaxLateness !== undefined) {
            latenessHTML += `<div class="metric-value">${scenarioData.achievedMaxLateness}</div>`;
            latenessHTML += '<div class="metric-label">days max lateness (achieved)</div>';
        } else {
            latenessHTML += `<div class="metric-value">${scenarioData.maxLateness || 0}</div>`;
            latenessHTML += '<div class="metric-label">days maximum lateness</div>';
        }
        latenessCard.innerHTML = latenessHTML;
    }

    const productGrid = document.getElementById('productGrid');
    productGrid.innerHTML = '';
    scenarioData.products.forEach(product => {
        const status = product.onTime ? 'on-time' :
            product.latenessDays <= 5 ? 'at-risk' : 'late';
        const card = document.createElement('div');
        card.className = 'product-card';
        card.innerHTML = `
            <div class="product-header">
                <div class="product-name">${product.name}</div>
                <div class="status-badge ${status}">${status.replace('-', ' ')}</div>
            </div>
            <div class="progress-bar">
                <div class="progress-fill" style="width: ${product.progress}%"></div>
            </div>
            <div class="product-stats">
                <span>📅 ${product.daysRemaining} days remaining</span>
                <span>⚡ ${product.criticalPath} critical tasks</span>
            </div>
            <div class="product-stats" style="margin-top: 5px; font-size: 11px;">
                <span>Tasks: ${product.totalTasks}</span>
                ${product.latePartsCount > 0 ? `<span>📦 Late Parts: ${product.latePartsCount}</span>` : ''}
                ${product.reworkCount > 0 ? `<span>🔧 Rework: ${product.reworkCount}</span>` : ''}
            </div>
            ${product.latenessDays > 0 ? `
                <div style="margin-top: 8px; padding: 5px; background: #fee2e2; border-radius: 4px; font-size: 12px; text-align: center;">
                    Late by ${product.latenessDays} days
                </div>
            ` : ''}
        `;
        card.style.cursor = 'pointer';
        card.addEventListener('click', () => showProductDetails(product.name));
        productGrid.appendChild(card);
    });

    const utilizationChart = document.getElementById('utilizationChart');
    utilizationChart.innerHTML = '';
    Object.entries(scenarioData.utilization).forEach(([team, utilization]) => {
        const item = document.createElement('div');
        item.className = 'utilization-item';
        let fillColor = 'linear-gradient(90deg, #10b981, #10b981)';
        if (utilization > 90) {
            fillColor = 'linear-gradient(90deg, #ef4444, #ef4444)';
        } else if (utilization > 75) {
            fillColor = 'linear-gradient(90deg, #f59e0b, #f59e0b)';
        }
        item.innerHTML = `
            <div class="team-label">${team}</div>
            <div class="utilization-bar">
                <div class="utilization-fill" style="width: ${utilization}%; background: ${fillColor};">
                    <span class="utilization-percent">${utilization}%</span>
                </div>
            </div>
        `;
        utilizationChart.appendChild(item);
    });
}

// Show product details (new feature)
async function showProductDetails(productName) {
    try {
        const response = await fetch(`/api/product/${productName}/tasks?scenario=${currentScenario}`);
        const data = await response.json();
        if (response.ok) {
            alert(`${productName}: ${data.totalTasks} total tasks\n` +
                `Production: ${data.taskBreakdown.Production || 0}\n` +
                `Quality: ${data.taskBreakdown['Quality Inspection'] || 0}\n` +
                `Late Parts: ${data.taskBreakdown['Late Part'] || 0}\n` +
                `Rework: ${data.taskBreakdown.Rework || 0}`);
        }
    } catch (error) {
        console.error('Error loading product details:', error);
    }
}

// Enhanced Individual Mechanic View
async function updateMechanicView() {
    if (!scenarioData) return;
    const mechanicId = document.getElementById('mechanicSelect').value;

    try {
        const response = await fetch(`/api/mechanic/${mechanicId}/tasks?scenario=${currentScenario}`);
        const data = await response.json();

        if (response.ok) {
            document.getElementById('currentShift').textContent = data.shift || '1st Shift';
            document.getElementById('tasksAssigned').textContent = data.tasks.length;

            if (data.tasks.length > 0) {
                const lastTask = data.tasks[data.tasks.length - 1];
                const endTime = new Date(lastTask.endTime);
                document.getElementById('estCompletion').textContent = formatTime(endTime);
            } else {
                document.getElementById('estCompletion').textContent = 'No tasks';
            }

            const timeline = document.getElementById('mechanicTimeline');
            timeline.innerHTML = '';

            data.tasks.forEach(task => {
                const startTime = new Date(task.startTime);
                const item = document.createElement('div');
                item.className = 'timeline-item';

                if (task.isLatePartTask) {
                    item.style.borderLeftColor = '#f59e0b';
                } else if (task.isReworkTask) {
                    item.style.borderLeftColor = '#ef4444';
                }

                let dependencyWarning = '';
                if (task.dependencies && task.dependencies.length > 0) {
                    const deps = task.dependencies.map(d => `${d.type} ${d.task}`).join(', ');
                    dependencyWarning = `
                        <div class="dependency-warning">
                            ⚠️ Waiting on: ${deps}
                        </div>
                    `;
                }

                let onDockInfo = '';
                if (task.onDockDate) {
                    const onDock = new Date(task.onDockDate);
                    onDockInfo = `<span>📦 On-dock: ${onDock.toLocaleDateString()}</span>`;
                }

                item.innerHTML = `
                    <div class="timeline-time">${formatTime(startTime)}</div>
                    <div class="timeline-content">
                        <div class="timeline-task">
                            Task ${task.taskId} - ${task.type}
                            ${task.isLatePartTask ? ' 📦' : ''}
                            ${task.isReworkTask ? ' 🔧' : ''}
                        </div>
                        <div class="timeline-details">
                            <span>📦 ${task.product}</span>
                            <span>⏱️ ${task.duration} minutes</span>
                            <span>👥 ${task.mechanics} mechanic(s)</span>
                            ${onDockInfo}
                        </div>
                        ${dependencyWarning}
                    </div>
                `;
                timeline.appendChild(item);
            });
        }
    } catch (error) {
        console.error('Error loading mechanic tasks:', error);
    }
}

// Helper functions
function getTaskTypeClass(type) {
    const typeMap = {
        'Production': 'production',
        'Quality Inspection': 'quality',
        'Late Part': 'late-part',
        'Rework': 'rework'
    };
    return typeMap[type] || 'production';
}

function getTaskTypeColor(type) {
    const colorMap = {
        'Production': '#10b981',
        'Quality Inspection': '#3b82f6',
        'Late Part': '#f59e0b',
        'Rework': '#ef4444'
    };
    return colorMap[type] || '#6b7280';
}

function formatTime(date) {
    return date.toLocaleTimeString('en-US', {
        hour: 'numeric',
        minute: '2-digit',
        hour12: true
    });
}

function formatDate(date) {
    return date.toLocaleDateString('en-US', {
        month: 'short',
        day: 'numeric',
        year: 'numeric'
    });
}

// Gantt chart helpers
function getGanttColor(product, isCritical) {
    const productColors = {
        'Product A': 'gantt-prod-a',
        'Product B': 'gantt-prod-b',
        'Product C': 'gantt-prod-c',
        'Product D': 'gantt-prod-d',
        'Product E': 'gantt-prod-e'
    };
    let classes = '';
    if (productColors[product]) {
        classes += productColors[product];
    }
    if (isCritical) {
        classes += ' gantt-critical';
    }
    return classes.trim();
}

function getGanttTasks(productFilter = 'all', teamFilter = 'all') {
    if (!scenarioData || !scenarioData.tasks) return [];
    return scenarioData.tasks
        .filter(task =>
            (productFilter === 'all' || task.product === productFilter) &&
            (teamFilter === 'all' || task.team === teamFilter)
        )
        .map(task => ({
            id: task.taskId,
            name: `${task.team} [ Task ${task.taskId} ] ${task.type}`,
            start: task.startTime,
            end: task.endTime,
            progress: 100,
            custom_class: getGanttColor(task.product, task.isCriticalPath),
            dependencies: (task.dependencies || []).map(d => d.task).join(','),
        }));
}

let gantt;
function renderGanttChart() {
    const productFilter = document.getElementById('ganttProductSelect').value || 'all';
    const teamFilter = document.getElementById('ganttTeamSelect').value || 'all';
    const tasks = getGanttTasks(productFilter, teamFilter);
    const ganttDiv = document.getElementById('ganttChart');
    ganttDiv.innerHTML = '';
    if (tasks.length === 0) {
        ganttDiv.innerHTML = '<div style="color: #ef4444;">No tasks to display.</div>';
        return;
    }
    gantt = new Gantt(ganttDiv, tasks, {
        view_mode: 'Day'
    });
}

function setupGanttProductFilter() {
    const select = document.getElementById('ganttProductSelect');
    select.innerHTML = '<option value="all">All Products</option>';
    if (scenarioData.products) {
        scenarioData.products.forEach(product => {
            const option = document.createElement('option');
            option.value = product.name;
            option.textContent = product.name;
            select.appendChild(option);
        });
    }
    select.onchange = renderGanttChart;
}

function setupGanttTeamFilter() {
    const select = document.getElementById('ganttTeamSelect');
    select.innerHTML = '<option value="all">All Teams</option>';
    if (scenarioData.tasks) {
        const teams = [...new Set(scenarioData.tasks.map(task => task.team))];
        teams.forEach(team => {
            const option = document.createElement('option');
            option.value = team;
            option.textContent = team;
            select.appendChild(option);
        });
    }
    select.onchange = renderGanttChart;
}

// Loading and error states
function showLoading(message = 'Loading...') {
    const content = document.querySelector('.main-content');
    if (content) {
        const loadingDiv = document.createElement('div');
        loadingDiv.id = 'loadingIndicator';
        loadingDiv.className = 'loading';
        loadingDiv.innerHTML = `
            <div style="text-align: center;">
                <div class="spinner"></div>
                <div style="margin-top: 20px;">${message}</div>
            </div>
        `;
        content.appendChild(loadingDiv);
    }
}

function hideLoading() {
    const loadingDiv = document.getElementById('loadingIndicator');
    if (loadingDiv) {
        loadingDiv.remove();
    }
}

function showError(message) {
    const content = document.querySelector('.main-content');
    if (content) {
        content.innerHTML = `
            <div style="text-align: center; padding: 40px; color: #ef4444;">
                <h2>Error</h2>
                <p>${message}</p>
                <button onclick="location.reload()" class="btn btn-primary" style="margin-top: 20px;">
                    Reload Page
                </button>
            </div>
        `;
    }
}

// Auto-assign function
async function autoAssign() {
    const selects = document.querySelectorAll('.assign-select');
    const mechanics = ['mech1', 'mech2', 'mech3', 'mech4'];
    let mechanicIndex = 0;
    let assignmentCount = 0;

    for (const select of selects) {
        const taskId = select.dataset.taskId;
        const mechanicId = mechanics[mechanicIndex % mechanics.length];

        try {
            const response = await fetch('/api/assign_task', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    taskId: taskId,
                    mechanicId: mechanicId,
                    scenario: currentScenario
                })
            });

            if (response.ok) {
                select.value = mechanicId;
                assignmentCount++;
            }
        } catch (error) {
            console.error('Error assigning task:', error);
        }

        mechanicIndex++;
    }

    alert(`Successfully assigned ${assignmentCount} tasks to mechanics!`);
}

// Export tasks function
async function exportTasks() {
    window.location.href = `/api/export/${currentScenario}`;
}

// Refresh data
async function refreshData() {
    if (confirm('This will recalculate all scenarios. It may take a few minutes. Continue?')) {
        showLoading('Refreshing all scenarios...');
        try {
            const response = await fetch('/api/refresh', { method: 'POST' });
            const result = await response.json();

            if (result.success) {
                await loadAllScenarios();
                alert('All scenarios refreshed successfully!');
            } else {
                alert('Failed to refresh: ' + result.error);
            }
        } catch (error) {
            alert('Error refreshing data: ' + error.message);
        } finally {
            hideLoading();
        }
    }
}

// Add refresh button to header if not exists
function setupRefreshButton() {
    const controls = document.querySelector('.controls');
    if (controls && !document.getElementById('refreshBtn')) {
        const refreshBtn = document.createElement('button');
        refreshBtn.id = 'refreshBtn';
        refreshBtn.className = 'btn btn-secondary';
        refreshBtn.innerHTML = '🔄 Refresh Data';
        refreshBtn.onclick = refreshData;
        refreshBtn.style.marginLeft = '10px';
        controls.appendChild(refreshBtn);
    }
}