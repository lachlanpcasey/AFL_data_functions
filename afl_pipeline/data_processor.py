"""
Data Processor module for AFL Data Pipeline.
Handles data transformations and preprocessing.
"""

import logging
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('afl_pipeline.data_processor')


def preprocess_player_stats(df, year):
    """
    Preprocess player statistics dataframe for storage.

    Args:
        df: pandas DataFrame with player statistics
        year: Year of the data

    Returns:
        pandas DataFrame: Processed dataframe
    """
    logger.info(f"Preprocessing player stats for year {year}")

    try:
        # Add year column
        df['year'] = year

        # Convert column types if needed
        type_conversions = {
            'Season': 'float',
            'Round': 'str',
            'Local.start.time': 'int',
            'Attendance': 'float',
            'ID': 'int',
            'Jumper.No.': 'str',
        }

        # Apply type conversions where possible
        for col, dtype in type_conversions.items():
            if col in df.columns:
                try:
                    df[col] = df[col].astype(dtype)
                except:
                    logger.warning(
                        f"Could not convert column {col} to {dtype}")

        # Drop the Date column if it exists
        if 'Date' in df.columns:
            df = df.drop('Date', axis=1)
            logger.info("Dropped 'Date' column")

        # Return the processed dataframe
        logger.info(
            f"Preprocessed dataframe has {len(df)} rows and {len(df.columns)} columns")
        return df

    except Exception as e:
        logger.error(f"Error preprocessing data: {str(e)}")
        # Return the original dataframe if there was an error
        return df


def create_sample_data(year):
    """
    Create sample player statistics data for testing.

    Args:
        year: Year to create sample data for

    Returns:
        pandas DataFrame: Sample data
    """
    logger.info(f"Creating sample data for year {year}")

    # Create a sample DataFrame that resembles AFL player statistics
    data = {
        'Season': [year] * 10,
        'Round': ['Round 1'] * 10,
        'Local.start.time': [1920] * 10,
        'Venue': ['M.C.G.'] * 10,
        'Attendance': [88084] * 10,
        'First.name': ['John', 'Michael', 'David', 'James', 'Robert', 'William', 'Richard', 'Joseph', 'Thomas', 'Charles'],
        'Surname': ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Miller', 'Davis', 'Garcia', 'Rodriguez', 'Wilson'],
        'ID': [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010],
        'Jumper.No.': [7, 9, 11, 13, 15, 17, 19, 21, 23, 25],
        'Playing.for': ['Richmond', 'Richmond', 'Richmond', 'Richmond', 'Richmond', 'Carlton', 'Carlton', 'Carlton', 'Carlton', 'Carlton'],
        'Kicks': [15, 12, 18, 10, 20, 14, 16, 13, 19, 17],
        'Marks': [6, 5, 8, 4, 9, 7, 8, 6, 10, 9],
        'Handballs': [10, 8, 12, 6, 15, 11, 14, 9, 16, 13],
        'Goals': [2, 1, 3, 0, 4, 2, 2, 1, 3, 2],
        'Behinds': [1, 2, 0, 1, 1, 0, 1, 0, 2, 1],
        'Hit.Outs': [0, 0, 20, 0, 0, 0, 0, 25, 0, 0],
        'Tackles': [5, 4, 7, 3, 8, 6, 7, 5, 9, 8],
        'Home.team': ['Richmond'] * 10,
        'Away.team': ['Carlton'] * 10,
        'year': [year] * 10
    }

    # Create a pandas DataFrame
    df = pd.DataFrame(data)
    logger.info(f"Created sample dataframe with {len(df)} rows")

    return df
