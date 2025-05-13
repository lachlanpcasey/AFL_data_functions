#!/usr/bin/env python
"""
Run all test cases for the AFL pipeline modules.
"""

import unittest
import sys
import os


def run_tests():
    """Run all test cases in the project."""
    # Get the tests directory path
    tests_dir = os.path.dirname(os.path.abspath(__file__))

    # Add the parent directory to sys.path to ensure imports work
    parent_dir = os.path.dirname(tests_dir)
    if parent_dir not in sys.path:
        sys.path.append(parent_dir)

    # Find and run all test modules in the tests directory
    test_loader = unittest.TestLoader()
    test_suite = test_loader.discover(tests_dir, pattern='*_test.py')

    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)

    # Return appropriate exit code
    return 0 if result.wasSuccessful() else 1


if __name__ == '__main__':
    sys.exit(run_tests())
