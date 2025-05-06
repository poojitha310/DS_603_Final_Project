#!/usr/bin/env python3
"""
Wrapper script to execute the road accident analysis pipeline.
"""

import os
import sys
import subprocess
import argparse
from datetime import datetime

def parse_arguments():
    """
    Parse command-line arguments.
    
    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(description='Road Accident Analysis Pipeline Wrapper')
    
    parser.add_argument('--all', action='store_true', 
                        help='Run the complete pipeline')
    
    parser.add_argument('--dask', action='store_true', 
                        help='Run Dask analysis')
    
    parser.add_argument('--spark', action='store_true', 
                        help='Run Spark ML pipeline')
    
    parser.add_argument('--sklearn', action='store_true', 
                        help='Run scikit-learn models')
    
    parser.add_argument('--sql', action='store_true', 
                        help='Run SQL analytics with DuckDB')
    
    parser.add_argument('--visualize', action='store_true', 
                        help='Create visualizations and dashboard')
    
    parser.add_argument('--sample', type=float, default=0.1, 
                        help='Fraction of data to sample (default: 0.1)')
    
    parser.add_argument('--data-path', type=str, default='data/US_Accidents.csv', 
                        help='Path to the CSV file (default: data/US_Accidents.csv)')
    
    parser.add_argument('--output-path', type=str, default='output', 
                        help='Path to save output (default: output)')
    
    return parser.parse_args()

def check_dependencies():
    """
    Check if all required dependencies are installed.
    
    Returns:
        bool: True if all dependencies are met, False otherwise
    """
    # Check for requirements.txt
    if not os.path.exists('requirements.txt'):
        print("ERROR: requirements.txt not found.")
        return False
    
    # Check if data directory exists
    if not os.path.exists('data'):
        print("Creating data directory...")
        os.makedirs('data', exist_ok=True)
    
    # Check if output directory exists
    if not os.path.exists('output'):
        print("Creating output directory...")
        os.makedirs('output', exist_ok=True)
        os.makedirs('output/visualizations', exist_ok=True)
        os.makedirs('output/sql_results', exist_ok=True)
    
    # Check if source files exist
    if not os.path.exists('src/main.py'):
        print("ERROR: Source files not found. Make sure you're in the project root directory.")
        return False
    
    return True

def run_pipeline(args):
    """
    Run the analysis pipeline based on arguments.
    
    Args:
        args: Command-line arguments
    """
    # Prepare command
    cmd = [sys.executable, 'src/main.py']
    
    # Add arguments
    if args.all:
        cmd.append('--all')
    if args.dask:
        cmd.append('--dask')
    if args.spark:
        cmd.append('--spark')
    if args.sklearn:
        cmd.append('--sklearn')
    if args.sql:
        cmd.append('--sql')
    if args.visualize:
        cmd.append('--visualize')
    
    cmd.extend(['--sample', str(args.sample)])
    cmd.extend(['--data-path', args.data_path])
    cmd.extend(['--output-path', args.output_path])
    
    # Print command
    print(f"Executing: {' '.join(cmd)}")
    
    # Run the command
    try:
        subprocess.run(cmd, check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Pipeline execution failed with error code {e.returncode}")
        return False

def check_data_file(data_path):
    """
    Check if the data file exists, and provide instructions if not.
    
    Args:
        data_path (str): Path to the data file
    
    Returns:
        bool: True if file exists, False otherwise
    """
    if os.path.exists(data_path):
        file_size = os.path.getsize(data_path) / (1024 * 1024)  # Size in MB
        print(f"Found data file: {data_path} ({file_size:.2f} MB)")
        return True
    else:
        print(f"ERROR: Data file not found at {data_path}")
        print("\nTo download the US Accidents dataset:")
        print("1. Visit https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents")
        print("2. Download 'US_Accidents.csv'")
        print(f"3. Place it in the {os.path.dirname(data_path)} directory")
        return False

def main():
    """
    Main function to run the pipeline wrapper.
    """
    print(f"{'='*80}")
    print(f"Road Accident Analysis Pipeline Wrapper")
    print(f"{'='*80}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Parse command-line arguments
    args = parse_arguments()
    
    # If no specific component is selected, run all
    if not (args.dask or args.spark or args.sklearn or args.sql or args.visualize):
        args.all = True
    
    # Check dependencies
    if not check_dependencies():
        sys.exit(1)
    
    # Check data file
    if not check_data_file(args.data_path):
        sys.exit(1)
    
    # Run the pipeline
    success = run_pipeline(args)
    
    if success:
        print("\nPipeline execution completed successfully.")
    else:
        print("\nPipeline execution failed. Check the error messages above.")
        sys.exit(1)
    
    print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}")

if __name__ == "__main__":
    main()