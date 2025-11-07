#!/usr/bin/env python3
"""
Plot PDR vs Latency Trade-off Analysis

This script creates a scatter plot showing the trade-off between Packet Delivery
Ratio (PDR) and latency across different network scenarios and configurations.

The plot helps demonstrate why adaptive RTO provides the best balance between
reliability and responsiveness.
"""

import json
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
from typing import Dict, List, Tuple


# Configuration for visual appearance
SCENARIO_COLORS = {
    'ideal': '#2ecc71',              # Green
    'typical_internet': '#3498db',   # Blue
    'high_congestion': '#f39c12',    # Orange
    'extreme_congestion': '#e74c3c'  # Red
}

SCENARIO_LABELS = {
    'ideal': 'Ideal (0% loss, 0ms delay)',
    'typical_internet': 'Typical (1% loss, 5ms delay)',
    'high_congestion': 'High Congestion (20% loss, 20ms delay)',
    'extreme_congestion': 'Extreme (40% loss, 50ms delay)'
}

# Configuration markers and labels
CONFIG_MARKERS = {
    'adaptive': 'o',      # Circle
    'rto25_skip200': '*',  # Star
    'rto50_skip200': '^',  # Triangle up
    'rto75_skip200': 'D',  # Diamond
    'rto100_skip200': 's', # Square
    'rto125_skip200': 'p', # Pentagon
    'rto150_skip200': 'v', # Triangle down
}

CONFIG_LABELS = {
    'adaptive': 'Adaptive RTO',
    'rto25_skip200': 'Static RTO=25ms',
    'rto50_skip200': 'Static RTO=50ms',
    'rto75_skip200': 'Static RTO=75ms',
    'rto100_skip200': 'Static RTO=100ms',
    'rto125_skip200': 'Static RTO=125ms',
    'rto150_skip200': 'Static RTO=150ms',
}


def load_test_results(results_dir: Path) -> Dict:
    """
    Load all test result JSON files from the performance_results directory.
    
    Returns:
        Dictionary mapping config_name -> test_data
    """
    results = {}
    
    # Find all JSON files in the results directory
    json_files = list(results_dir.glob('performance_test_*.json'))
    
    for json_file in json_files:
        try:
            with open(json_file, 'r') as f:
                data = json.load(f)
            
            # Determine config name from filename or data
            if data.get('adaptive'):
                config_name = 'adaptive'
            else:
                rto = data.get('rto_ms', 100)
                skip = data.get('skip_threshold', 200)
                config_name = f'rto{rto}_skip{skip}'
            
            results[config_name] = data
            print(f"Loaded: {json_file.name} -> {config_name}")
            
        except Exception as e:
            print(f"Warning: Could not load {json_file.name}: {e}")
    
    return results


def extract_data_points(results: Dict) -> List[Dict]:
    """
    Extract all data points for plotting.
    
    Returns:
        List of dictionaries with keys: config, scenario, channel, pdr, latency
    """
    data_points = []
    
    for config_name, test_data in results.items():
        scenarios = test_data.get('scenarios', {})
        
        for scenario_name, scenario_data in scenarios.items():
            metrics = scenario_data.get('metrics', {})
            
            # Extract unreliable channel data
            if 'unreliable' in metrics:
                unreliable = metrics['unreliable']
                data_points.append({
                    'config': config_name,
                    'scenario': scenario_name,
                    'channel': 'unreliable',
                    'pdr': unreliable.get('pdr', 0),
                    'latency': unreliable.get('latency', 0)
                })
            
            # Extract reliable channel data
            if 'reliable' in metrics:
                reliable = metrics['reliable']
                data_points.append({
                    'config': config_name,
                    'scenario': scenario_name,
                    'channel': 'reliable',
                    'pdr': reliable.get('pdr', 0),
                    'latency': reliable.get('latency', 0)
                })
    
    return data_points


def create_tradeoff_plot(data_points: List[Dict], output_file: str = 'pdr_latency_tradeoff.png'):
    """
    Create a comprehensive scatter plot showing PDR vs Latency trade-offs.
    
    Args:
        data_points: List of data point dictionaries
        output_file: Output filename for the plot
    """
    # Set up the plot with a clean style
    plt.style.use('seaborn-v0_8-darkgrid')
    fig, ax = plt.subplots(figsize=(16, 10))
    
    # Group data by scenario for plotting
    scenarios = sorted(set(point['scenario'] for point in data_points))
    configs = sorted(set(point['config'] for point in data_points))
    
    # Plot each configuration and scenario combination
    for scenario in scenarios:
        for config in configs:
            # Get reliable channel data for this scenario/config combination
            reliable_points = [
                p for p in data_points 
                if p['scenario'] == scenario and p['config'] == config and p['channel'] == 'reliable'
            ]
            
            if not reliable_points:
                continue
            
            # Extract PDR and latency values
            pdrs = [p['pdr'] for p in reliable_points]
            latencies = [p['latency'] for p in reliable_points]
            
            # Get visual properties
            color = SCENARIO_COLORS.get(scenario, '#95a5a6')
            marker = CONFIG_MARKERS.get(config, 'o')
            
            # Plot the point (solid fill for reliable channel only)
            ax.scatter(latencies, pdrs, 
                      marker=marker, 
                      s=200,  # Size of markers
                      color=color,
                      edgecolors='black',
                      linewidths=1.0,
                      alpha=0.8,
                      zorder=3)
    
    # Add optimal zone (high PDR, low latency) - top-left corner
    ax.axhspan(95, 101, alpha=0.15, color='green', zorder=1)
    ax.text(5, 97.5, 'Optimal Zone\n(High PDR, Low Latency)', 
            fontsize=11, color='green', alpha=0.7, weight='bold',
            bbox=dict(boxstyle='round,pad=0.5', facecolor='white', alpha=0.7))
    
    # Formatting
    ax.set_xlabel('Latency (ms)', fontsize=14, weight='bold')
    ax.set_ylabel('Packet Delivery Ratio (PDR) %', fontsize=14, weight='bold')
    ax.set_title('PDR vs Latency Trade-off: Reliable Channel Performance\nAcross Network Scenarios and RTO Configurations', 
                 fontsize=16, weight='bold', pad=20)
    
    # Set axis limits with some padding
    all_latencies = [p['latency'] for p in data_points if p['channel'] == 'reliable']
    all_pdrs = [p['pdr'] for p in data_points if p['channel'] == 'reliable']
    
    if all_latencies and all_pdrs:
        max_latency = max(all_latencies)
        min_pdr = min(all_pdrs)
        
        ax.set_ylim(max(50, min_pdr - 5), 101)
        ax.set_xlim(-2, max_latency * 1.1)
    
    # Add grid
    ax.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
    
    # Create custom legend with better organization
    from matplotlib.patches import Patch
    from matplotlib.lines import Line2D
    
    # Legend for scenarios (colors)
    scenario_patches = [
        Patch(facecolor=SCENARIO_COLORS.get(s, '#95a5a6'), 
              edgecolor='black', label=SCENARIO_LABELS.get(s, s)) 
        for s in scenarios if s in SCENARIO_LABELS
    ]
    
    # Legend for configurations (markers)
    config_lines = [
        Line2D([0], [0], marker=CONFIG_MARKERS.get(c, 'o'), color='gray', 
               linestyle='', markersize=12, markeredgecolor='black',
               markeredgewidth=1, label=CONFIG_LABELS.get(c, c))
        for c in configs if c in CONFIG_LABELS
    ]
    
    # Place legends
    if scenario_patches:
        legend1 = ax.legend(handles=scenario_patches, loc='lower left', 
                           title='Network Scenarios', framealpha=0.95, 
                           fontsize=10, title_fontsize=11)
        ax.add_artist(legend1)
    
    if config_lines:
        legend2 = ax.legend(handles=config_lines, loc='lower right', 
                           title='RTO Configurations', framealpha=0.95,
                           fontsize=10, title_fontsize=11, ncol=1)
    
    # Tight layout
    plt.tight_layout()    # Save the plot
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"\n✅ Plot saved to: {output_file}")
    
    # Show the plot
    plt.show()


def print_summary_statistics(data_points: List[Dict]):
    """
    Print summary statistics for each configuration.
    """
    print("\n" + "="*80)
    print("SUMMARY STATISTICS")
    print("="*80)
    
    configs = set(point['config'] for point in data_points)
    
    for config in sorted(configs):
        config_points = [p for p in data_points if p['config'] == config and p['channel'] == 'reliable']
        
        if not config_points:
            continue
        
        avg_pdr = np.mean([p['pdr'] for p in config_points])
        avg_latency = np.mean([p['latency'] for p in config_points])
        
        print(f"\n{CONFIG_LABELS.get(config, config)}:")
        print(f"  Average Reliable PDR: {avg_pdr:.1f}%")
        print(f"  Average Reliable Latency: {avg_latency:.1f}ms")
        
        # Show per-scenario breakdown
        scenarios = set(p['scenario'] for p in config_points)
        for scenario in sorted(scenarios):
            scenario_points = [p for p in config_points if p['scenario'] == scenario]
            if scenario_points:
                pdr = scenario_points[0]['pdr']
                latency = scenario_points[0]['latency']
                print(f"    {SCENARIO_LABELS.get(scenario, scenario)}: PDR={pdr:.1f}%, Latency={latency:.1f}ms")


def create_scenario_plot(data_points: List[Dict], scenario: str, output_file: str):
    """
    Create a detailed plot for a single scenario showing RTO configurations.
    
    Args:
        data_points: List of data point dictionaries
        scenario: Scenario name to plot
        output_file: Output filename for the plot
    """
    # Filter data for this scenario (reliable channel only)
    scenario_data = [
        p for p in data_points 
        if p['scenario'] == scenario and p['channel'] == 'reliable'
    ]
    
    if not scenario_data:
        print(f"⚠️  No data for scenario: {scenario}")
        return
    
    # Set up the plot
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Group by configuration
    configs = sorted(set(p['config'] for p in scenario_data))
    
    for config in configs:
        config_points = [p for p in scenario_data if p['config'] == config]
        
        if not config_points:
            continue
        
        latencies = [p['latency'] for p in config_points]
        pdrs = [p['pdr'] for p in config_points]
        
        marker = CONFIG_MARKERS.get(config, 'o')
        color = SCENARIO_COLORS.get(scenario, '#3498db')
        label = CONFIG_LABELS.get(config, config)
        
        # Plot with larger markers
        ax.scatter(latencies, pdrs, 
                  marker=marker, 
                  s=300,
                  color=color,
                  edgecolors='black',
                  linewidths=2,
                  alpha=0.85,
                  label=label,
                  zorder=3)
        
        # Add text labels showing exact values
        for lat, pdr in zip(latencies, pdrs):
            ax.annotate(f'{lat:.1f}ms\n{pdr:.1f}%',
                       xy=(lat, pdr),
                       xytext=(10, 10),
                       textcoords='offset points',
                       fontsize=9,
                       bbox=dict(boxstyle='round,pad=0.3', 
                                facecolor='white', 
                                edgecolor='gray',
                                alpha=0.8),
                       ha='left')
    
    # Add optimal zone
    ax.axhspan(95, 101, alpha=0.15, color='green', zorder=1)
    
    # Formatting
    ax.set_xlabel('Latency (ms)', fontsize=13, weight='bold')
    ax.set_ylabel('Packet Delivery Ratio (PDR) %', fontsize=13, weight='bold')
    
    scenario_label = SCENARIO_LABELS.get(scenario, scenario)
    ax.set_title(f'{scenario_label}\nReliable Channel Performance', 
                 fontsize=15, weight='bold', pad=20)
    
    # Adjust axis limits to zoom in on the data
    all_latencies = [p['latency'] for p in scenario_data]
    all_pdrs = [p['pdr'] for p in scenario_data]
    
    if all_latencies and all_pdrs:
        lat_min, lat_max = min(all_latencies), max(all_latencies)
        pdr_min, pdr_max = min(all_pdrs), max(all_pdrs)
        
        # Add padding
        lat_range = lat_max - lat_min
        pdr_range = pdr_max - pdr_min
        
        # Set limits with appropriate padding
        ax.set_xlim(max(0, lat_min - lat_range * 0.15), 
                   lat_max + lat_range * 0.4)  # Extra space for annotations
        ax.set_ylim(max(90, pdr_min - max(2, pdr_range * 0.2)), 
                   min(101, pdr_max + max(1, pdr_range * 0.1)))
    
    # Add grid
    ax.grid(True, alpha=0.4, linestyle='--', linewidth=0.8)
    ax.set_axisbelow(True)
    
    # Legend in bottom right
    ax.legend(loc='lower right', framealpha=0.95, fontsize=11, 
             title='RTO Configuration', title_fontsize=12)
    
    # Tight layout
    plt.tight_layout()
    
    # Save
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✅ Saved: {output_file}")
    
    plt.close()


def main():
    """Main execution function."""
    print("="*80)
    print("PDR vs Latency Trade-off Analysis")
    print("="*80)
    
    # Set up paths
    script_dir = Path(__file__).parent
    results_dir = script_dir / 'performance_results'
    
    # Check if results directory exists
    if not results_dir.exists():
        print(f"❌ Error: Results directory not found: {results_dir}")
        print("Please run experiments first using run_targeted_experiments.py")
        return
    
    # Load all test results
    print(f"\nLoading test results from: {results_dir}")
    results = load_test_results(results_dir)
    
    if not results:
        print("❌ Error: No test results found!")
        print("Please run experiments first using run_targeted_experiments.py")
        return
    
    print(f"\n✅ Loaded {len(results)} test configurations")
    
    # Extract data points
    data_points = extract_data_points(results)
    print(f"✅ Extracted {len(data_points)} data points")
    
    # Create the overall trade-off plot
    output_file = script_dir / 'pdr_latency_tradeoff_analysis.png'
    create_tradeoff_plot(data_points, str(output_file))
    
    # Create individual plots for each non-ideal scenario
    print("\n" + "="*80)
    print("Generating individual scenario plots (non-ideal scenarios only)...")
    print("="*80 + "\n")
    
    all_scenarios = sorted(set(p['scenario'] for p in data_points))
    non_ideal_scenarios = [s for s in all_scenarios if s != 'ideal']
    
    for scenario in non_ideal_scenarios:
        scenario_output = script_dir / f'plot_{scenario}.png'
        create_scenario_plot(data_points, scenario, str(scenario_output))
    
    # Print summary statistics
    print("\n")
    print_summary_statistics(data_points)
    
    print("\n" + "="*80)
    print("Analysis complete!")
    print("="*80)


if __name__ == '__main__':
    main()
