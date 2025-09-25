#!/usr/bin/env python3
"""
Backend log error checker for e2e tests.

This script checks backend logs for errors during e2e test execution.
It should be run after e2e tests to identify any backend issues.
"""

import os
import re
from datetime import datetime, timedelta
from typing import List, Dict

def check_log_file(log_file: str, since_minutes: int = 5) -> List[Dict[str, str]]:
    """Check a log file for errors since the specified time."""
    if not os.path.exists(log_file):
        return []
    
    errors = []
    cutoff_time = datetime.now() - timedelta(minutes=since_minutes)
    
    with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            # Parse timestamp from log line
            timestamp_match = re.match(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
            if timestamp_match:
                try:
                    log_time = datetime.strptime(timestamp_match.group(1), '%Y-%m-%d %H:%M:%S')
                    if log_time < cutoff_time:
                        continue
                except ValueError:
                    continue
            
            # Check for error patterns
            if any(keyword in line.lower() for keyword in ['error', 'exception', 'traceback', 'failed']):
                errors.append({
                    'file': log_file,
                    'line': line.strip(),
                    'timestamp': timestamp_match.group(1) if timestamp_match else 'unknown'
                })
    
    return errors

def main():
    """Main function to check all backend log files."""
    log_files = ['app.log', 'app_stderr.log', 'app_stdout.log']
    
    print("=== BACKEND LOG ERROR CHECK ===")
    print(f"Checking logs since {datetime.now() - timedelta(minutes=5)}")
    print()
    
    all_errors = []
    
    for log_file in log_files:
        if os.path.exists(log_file):
            print(f"Checking {log_file}...")
            errors = check_log_file(log_file, since_minutes=5)
            all_errors.extend(errors)
            
            if errors:
                print(f"  Found {len(errors)} potential errors:")
                for error in errors:
                    print(f"    [{error['timestamp']}] {error['line']}")
            else:
                print("  No errors found")
        else:
            print(f"{log_file} not found")
    
    print()
    
    if all_errors:
        print(f"=== SUMMARY: {len(all_errors)} potential errors found ===")
        
        # Save errors to file
        with open('backend_log_errors.json', 'w', encoding='utf-8') as f:
            import json
            json.dump(all_errors, f, indent=2, ensure_ascii=False)
        
        print("Errors saved to backend_log_errors.json")
        
        # Check for timing-related errors
        timing_errors = [e for e in all_errors if any(keyword in e['line'].lower() 
                        for keyword in ['timing', 'monotonic', 'start_time', 'end_time'])]
        
        if timing_errors:
            print(f"\n=== TIMING-RELATED ERRORS ({len(timing_errors)}): ===")
            for error in timing_errors:
                print(f"  [{error['timestamp']}] {error['line']}")
    else:
        print("✅ No errors found in backend logs")

if __name__ == '__main__':
    main()
