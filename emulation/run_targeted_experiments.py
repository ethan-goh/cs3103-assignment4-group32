#!/usr/bin/env python3
"""
HUDP Performance Test Across Network Conditions

This script tests HUDP with adaptive parameters across three network scenarios:
1. Ideal Network - 0% loss, 4ms delay
2. Typical Internet - 1% loss, 4ms delay  
3. High Congestion - 20% loss, 35ms delay

Results are saved to JSON for analysis and plotting.

Usage:
    cd emulation/
    sudo python3 run_targeted_experiments.py [--adaptive] [--rto RTO] [--skip SKIP]
"""

from mininet.net import Mininet
from mininet.node import OVSController
from mininet.link import TCLink
from mininet.log import setLogLevel, info
import time
import os
import json
import re
import argparse
from datetime import datetime


class NetworkPerformanceTest:
    """Test HUDP performance across different network conditions."""
    
    def __init__(self, output_dir='performance_results', adaptive=False, rto_ms=100, skip_threshold=200):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        self.project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        self.adaptive = adaptive
        self.rto_ms = rto_ms
        self.skip_threshold = skip_threshold
        
    def create_network(self, delay='0ms', loss=0, bandwidth=None):
        """Create network topology with proper TC enforcement."""
        try:
            net = Mininet(controller=OVSController, link=TCLink)
        except:
            net = Mininet(controller=None, link=TCLink)
        
        if net.controller:
            try:
                net.addController('c0')
            except:
                pass
        
        h1 = net.addHost('h1', ip='10.0.0.1/24')
        h2 = net.addHost('h2', ip='10.0.0.2/24')
        
        # Configure link parameters
        link_params = {'delay': delay, 'loss': loss}
        if bandwidth:
            link_params['bw'] = bandwidth
            link_params['use_htb'] = True
            if bandwidth >= 100:
                link_params['max_queue_size'] = 1000
        
        # Add a minimal switch to ensure TC rules are applied
        # Direct links sometimes bypass netem in Mininet
        s1 = net.addSwitch('s1', failMode='standalone')
        
        # Apply network conditions on BOTH links
        net.addLink(h1, s1, **link_params)
        net.addLink(h2, s1, **link_params)
        
        return net, h1, h2
    
    def parse_final_metrics(self, log_content):
        """Parse final metrics with anomaly detection and correction."""
        metrics = {'unreliable': {}, 'reliable': {}}
        
        lines = log_content.strip().split('\n')
        
        # Work backwards from end of file
        for i in range(len(lines) - 1, -1, -1):
            line = lines[i]
            
            if 'UNREL:' in line:
                match = re.search(r'PDR=([\d.]+)%\s+Lat=([\d.]+)ms\s+Jitter=([\d.]+)ms\s+Thru=([\d.]+)B/s\s+Sent=(\d+)\s+Recv=(\d+)', line)
                if match:
                    pdr = float(match.group(1))
                    latency = float(match.group(2))
                    jitter = float(match.group(3))
                    throughput = float(match.group(4))
                    sent = int(match.group(5))
                    received = int(match.group(6))
                    
                    # Anomaly detection and correction
                    # Cap PDR at 100% (STATS_SYNC lag artifact)
                    if pdr > 100:
                        pdr = 100.0
                    
                    # Detect throughput anomalies (> 10KB/s is suspicious for our test)
                    if throughput > 10000:  # 10KB/s threshold
                        throughput = 0.0  # Mark as invalid
                    
                    # Detect latency anomalies (> 500ms indicates failure)
                    if latency > 500:
                        latency = -1.0  # Mark as failure indicator
                    
                    metrics['unreliable'] = {
                        'pdr': pdr,
                        'latency': latency,
                        'jitter': jitter,
                        'throughput': throughput,
                        'sent': sent,
                        'received': received
                    }
            
            elif 'REL:' in line:
                match = re.search(r'PDR=([\d.]+)%\s+Lat=([\d.]+)ms\s+Jitter=([\d.]+)ms\s+Thru=([\d.]+)B/s\s+Sent=(\d+)\s+Recv=(\d+)', line)
                if match:
                    pdr = float(match.group(1))
                    latency = float(match.group(2))
                    jitter = float(match.group(3))
                    throughput = float(match.group(4))
                    sent = int(match.group(5))
                    received = int(match.group(6))
                    
                    # Anomaly detection and correction
                    if pdr > 100:
                        pdr = 100.0
                    
                    if throughput > 10000:
                        throughput = 0.0
                    
                    if latency > 500:
                        latency = -1.0
                    
                    metrics['reliable'] = {
                        'pdr': pdr,
                        'latency': latency,
                        'jitter': jitter,
                        'throughput': throughput,
                        'sent': sent,
                        'received': received
                    }
            
            if metrics['unreliable'] and metrics['reliable']:
                break
        
        return metrics
    
    def run_test(self, scenario_name, network_params, test_params):
        """Run a single test and return parsed metrics."""
        adaptive_str = "ADAPTIVE" if self.adaptive else f"RTO={self.rto_ms}/Skip={self.skip_threshold}"
        info(f'\n*** {scenario_name}: {adaptive_str} ***\n')
        
        net, h1, h2 = self.create_network(**network_params)
        net.start()
        time.sleep(2)
        
        temp_log = f'/tmp/hudp_server_{scenario_name}.log'
        
        info('*** Starting server\n')
        adaptive_flag = '--adaptive' if self.adaptive else ''
        h2.cmd(f'cd {self.project_root} && python3 -u -m application.game_server '
               f'--listen-host 10.0.0.2 --listen-port 5000 '
               f'--rto {self.rto_ms} --skip-threshold {self.skip_threshold} '
               f'{adaptive_flag} '
               f'> {temp_log} 2>&1 &')
        time.sleep(5)  # Increased wait for server + allow connection establishment under loss
        
        # Verify server is running
        server_check = h2.cmd('pgrep -f "application.game_server"')
        if not server_check.strip():
            info('*** WARNING: Server process not found! Check logs. ***\n')
        
        rate = test_params.get('rate', 50)
        reliable_prob = test_params.get('reliable_prob', 0.2)
        duration = test_params.get('duration', 30)
        
        # Temporary client log for debugging
        temp_client_log = f'/tmp/hudp_client_{scenario_name}.log'
        
        info(f'*** Running client for {duration}s ***\n')
        h1.cmd(f'cd {self.project_root} && '
               f'timeout {duration} python3 -u -m application.game_client '
               f'--server-host 10.0.0.2 --server-port 5000 '
               f'--rate {rate} --reliable-prob {reliable_prob} '
               f'--rto {self.rto_ms} --skip-threshold {self.skip_threshold} '
               f'{adaptive_flag} '
               f'> {temp_client_log} 2>&1')
        
        # DEBUG: Print client log to see why it died
        info('*** Client log (last 20 lines): ***\n')
        client_log_tail = h1.cmd(f'tail -20 {temp_client_log} 2>/dev/null')
        info(client_log_tail + '\n')
        
        time.sleep(1)
        h2.cmd('pkill -TERM -f "application.game_server"')
        time.sleep(1)
        h2.cmd('pkill -9 -f "application.game_server" 2>/dev/null')
        
        # DEBUG: Print last 20 lines of server log
        info('*** Server log (last 20 lines): ***\n')
        log_tail = h2.cmd(f'tail -20 {temp_log} 2>/dev/null')
        info(log_tail + '\n')
        
        metrics = {}
        try:
            with open(temp_log, 'r') as f:
                log_content = f.read()
            metrics = self.parse_final_metrics(log_content)
            
            # Calculate quality score
            rel = metrics.get('reliable', {})
            score = 0
            if rel.get('pdr', 0) >= 95:
                score += 40
            if rel.get('latency', 999) < 100 and rel.get('latency', -1) > 0:
                score += 30
            if rel.get('jitter', 999) < 5:
                score += 20
            if rel.get('throughput', 0) > 500:
                score += 10
            
            info(f'*** Score: {score}/100, PDR={rel.get("pdr", 0):.1f}%, '
                 f'Lat={rel.get("latency", 0):.1f}ms ***\n')
        except Exception as e:
            info(f'*** Warning: {e} ***\n')
            metrics = {'unreliable': {}, 'reliable': {}}
        
        try:
            os.remove(temp_log)
        except:
            pass
        
        try:
            net.stop()
        except:
            pass
        time.sleep(1)
        
        return metrics
    
    def run_performance_suite(self):
        """Run HUDP across four network scenarios."""
        
        scenarios = [
            {
                'name': 'ideal',
                'network': {'delay': '0ms', 'loss': 0, 'bandwidth': 2},
                'test': {'rate': 50, 'reliable_prob': 0.5, 'duration': 15}
            },
            {
                'name': 'typical_internet',
                'network': {'delay': '5ms', 'loss': 1, 'bandwidth': 2},
                'test': {'rate': 50, 'reliable_prob': 0.5, 'duration': 45}
            },
            {
                'name': 'high_congestion',
                'network': {'delay': '20ms', 'loss': 20, 'bandwidth': 2},
                'test': {'rate': 50, 'reliable_prob': 0.5, 'duration': 45}
            },
        ]
        
        results = {
            'timestamp': datetime.now().isoformat(),
            'adaptive': self.adaptive,
            'rto_ms': self.rto_ms,
            'skip_threshold': self.skip_threshold,
            'scenarios': {}
        }
        
        mode_desc = "Adaptive Mode" if self.adaptive else f"Static (RTO={self.rto_ms}ms, Skip={self.skip_threshold}ms)"
        info(f'\n*** Running HUDP Performance Tests - {mode_desc} ***\n')
        
        for scenario in scenarios:
            try:
                info(f'\n{"="*70}\n')
                info(f'Scenario: {scenario["name"].upper().replace("_", " ")}\n')
                info(f'{"="*70}\n')
                
                metrics = self.run_test(
                    scenario['name'],
                    scenario['network'],
                    scenario['test']
                )
                
                results['scenarios'][scenario['name']] = {
                    'network': scenario['network'],
                    'metrics': metrics
                }
                
            except Exception as e:
                info(f'*** Error: {e} ***\n')
                results['scenarios'][scenario['name']] = {'error': str(e)}
        
        return results
    
    def save_results(self, results):
        """Save results to JSON."""
        mode_suffix = "adaptive" if self.adaptive else f"rto{self.rto_ms}_skip{self.skip_threshold}"
        filename = f'performance_test_{mode_suffix}.json'
        filepath = os.path.join(self.output_dir, filename)
        
        with open(filepath, 'w') as f:
            json.dump(results, f, indent=2)
        
        info(f'\n*** Results saved to {filepath} ***\n')
        return filepath
    
    def display_summary(self, results):
        """Display summary of results."""
        info('\n' + '='*80 + '\n')
        info('📊 PERFORMANCE SUMMARY\n')
        info('='*80 + '\n')
        
        if results.get('adaptive'):
            info('Mode: Adaptive (RTO and Skip adjust to network conditions)\n')
        else:
            info(f"Mode: Static (RTO={results['rto_ms']}ms, Skip={results['skip_threshold']}ms)\n")
        info('\n')
        
        for scenario_name, scenario_data in results['scenarios'].items():
            if 'error' in scenario_data:
                info(f"❌ {scenario_name.upper().replace('_', ' ')}: ERROR\n\n")
                continue
            
            metrics = scenario_data.get('metrics', {})
            network = scenario_data.get('network', {})
            
            info(f"� {scenario_name.upper().replace('_', ' ')}\n")
            info(f"   Network: {network.get('delay', 'N/A')} delay, "
                 f"{network.get('loss', 0)}% loss\n")
            info('-' * 80 + '\n')
            
            for chan_type in ['reliable', 'unreliable']:
                chan_metrics = metrics.get(chan_type, {})
                if not chan_metrics:
                    continue
                
                icon = '🔒' if chan_type == 'reliable' else '📤'
                chan_name = 'RELIABLE' if chan_type == 'reliable' else 'UNRELIABLE'
                
                info(f"   {icon} {chan_name}: ")
                info(f"PDR={chan_metrics.get('pdr', 0):.1f}%, ")
                info(f"Lat={chan_metrics.get('latency', 0):.1f}ms, ")
                info(f"Jitter={chan_metrics.get('jitter', 0):.2f}ms, ")
                info(f"Thru={chan_metrics.get('throughput', 0):.0f}B/s\n")
            
            info('\n')
        
        info('='*80 + '\n')


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Test H-UDP performance across three network scenarios'
    )
    parser.add_argument('--adaptive', action='store_true',
                       help='Enable adaptive RTO and skip threshold tuning')
    parser.add_argument('--rto', type=int, default=100,
                       help='Static RTO in ms (ignored if --adaptive)')
    parser.add_argument('--skip', type=int, default=200,
                       help='Static skip threshold in ms (ignored if --adaptive)')
    parser.add_argument('--output-dir', default='performance_results',
                       help='Directory for results (default: performance_results)')
    
    args = parser.parse_args()
    
    setLogLevel('info')
    
    info('\n' + '='*70 + '\n')
    info('H-UDP Network Performance Testing\n')
    if args.adaptive:
        info('Mode: Adaptive Parameter Tuning\n')
    else:
        info(f'Mode: Static (RTO={args.rto}ms, Skip={args.skip}ms)\n')
    info('='*70 + '\n\n')
    
    test = NetworkPerformanceTest(
        output_dir=args.output_dir,
        adaptive=args.adaptive,
        rto_ms=args.rto,
        skip_threshold=args.skip
    )
    
    results = test.run_performance_suite()
    filepath = test.save_results(results)
    test.display_summary(results)
    
    info('\n' + '='*70 + '\n')
    info('Testing complete!\n')
    info(f'Results saved to: {filepath}\n')
    info('='*70 + '\n')


if __name__ == '__main__':
    main()
