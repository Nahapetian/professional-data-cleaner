"""
Professional Data Cleaner - Core Module
A comprehensive data cleaning and analysis tool for Excel/CSV files
"""

import pandas as pd
import numpy as np
import re
import os
from pathlib import Path
import warnings
import psutil
import gc
from typing import Dict, List, Optional, Union, Tuple
from abc import ABC, abstractmethod

warnings.filterwarnings('ignore')

class DataProfile:
    """Data profiling and analysis class"""

    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.profile = {}

    def create_profile(self) -> Dict:
        """Create comprehensive data profile"""
        self.profile = {
            'basic_info': self._get_basic_info(),
            'data_quality': self._get_data_quality(),
            'column_analysis': self._get_column_analysis(),
            'data_types': self.df.dtypes.value_counts().to_dict()
        }
        return self.profile

    def _get_basic_info(self) -> Dict:
        """Get basic dataset information"""
        return {
            'total_rows': len(self.df),
            'total_columns': len(self.df.columns),
            'total_cells': self.df.size,
            'memory_usage_mb': self.df.memory_usage(deep=True).sum() / 1024**2
        }

    def _get_data_quality(self) -> Dict:
        """Get data quality metrics"""
        return {
            'missing_values_total': self.df.isnull().sum().sum(),
            'duplicate_rows': self.df.duplicated().sum(),
            'completeness_percentage': ((self.df.size - self.df.isnull().sum().sum()) / self.df.size * 100)
        }

    def _get_column_analysis(self) -> Dict:
        """Analyze each column in detail"""
        column_analysis = {}

        for col in self.df.columns:
            col_info = {
                'data_type': str(self.df[col].dtype),
                'missing_count': self.df[col].isnull().sum(),
                'missing_percentage': (self.df[col].isnull().sum() / len(self.df)) * 100,
                'unique_values': self.df[col].nunique(),
                'unique_percentage': (self.df[col].nunique() / len(self.df)) * 100
            }

            # Add numeric statistics
            if pd.api.types.is_numeric_dtype(self.df[col]):
                col_info.update(self._get_numeric_stats(col))

            # Add text statistics
            elif self.df[col].dtype == 'object':
                col_info.update(self._get_text_stats(col))

            column_analysis[col] = col_info

        return column_analysis

    def _get_numeric_stats(self, column: str) -> Dict:
        """Get statistics for numeric columns"""
        try:
            valid_data = self.df[column].dropna()
            if len(valid_data) > 0:
                return {
                    'min_value': float(valid_data.min()),
                    'max_value': float(valid_data.max()),
                    'mean': float(valid_data.mean()),
                    'median': float(valid_data.median()),
                    'std_deviation': float(valid_data.std()) if len(valid_data) > 1 else 0.0
                }
        except:
            pass
        return {'min_value': 'N/A', 'max_value': 'N/A', 'mean': 'N/A', 'median': 'N/A', 'std_deviation': 'N/A'}

    def _get_text_stats(self, column: str) -> Dict:
        """Get statistics for text columns"""
        try:
            mode_val = self.df[column].mode()
            most_frequent = mode_val.iloc[0] if not mode_val.empty else 'N/A'
            avg_length = self.df[column].astype(str).str.len().mean()

            return {
                'most_frequent_value': str(most_frequent),
                'avg_text_length': float(avg_length) if not pd.isna(avg_length) else 0.0
            }
        except:
            return {'most_frequent_value': 'Error', 'avg_text_length': 0.0}

class SystemResourceManager:
    """System resource monitoring and management"""

    @staticmethod
    def check_system_resources() -> bool:
        """Check if system has sufficient resources"""
        memory = psutil.virtual_memory()
        available_gb = memory.available / (1024**3)
        total_gb = memory.total / (1024**3)

        print(f"💻 System Memory:")
        print(f"   Total RAM: {total_gb:.1f} GB")
        print(f"   Available RAM: {available_gb:.1f} GB")
        print(f"   Used: {memory.percent}%")

        return available_gb > 2.0

    @staticmethod
    def estimate_memory_usage(file_path: str) -> float:
        """Estimate memory usage for file processing"""
        try:
            file_size_mb = os.path.getsize(file_path) / (1024**2)
            estimated_memory_gb = (file_size_mb * 4) / 1024

            print(f"📊 File Size: {file_size_mb:.1f} MB")
            print(f"📊 Estimated Memory Need: {estimated_memory_gb:.1f} GB")

            return estimated_memory_gb
        except:
            return 0

class DataLoader:
    """Data loading and file handling"""

    def __init__(self, file_path: str):
        self.file_path = file_path

    def load_data(self) -> Optional[pd.DataFrame]:
        """Load data from various file formats"""
        try:
            file_extension = Path(self.file_path).suffix.lower()

            if file_extension in ['.xlsx', '.xls']:
                df = pd.read_excel(self.file_path)
                print(f"✅ Excel file loaded successfully")

            elif file_extension == '.csv':
                df = self._load_csv()
                print(f"✅ CSV file loaded successfully")

            elif file_extension == '.pdf':
                print("⚠️ PDF files require additional libraries")
                return None

            else:
                print(f"❌ Unsupported file format: {file_extension}")
                return None

            print(f"📊 Data loaded: {df.shape[0]} rows, {df.shape[1]} columns")
            return df

        except Exception as e:
            print(f"❌ Error loading file: {str(e)}")
            return None

    def _load_csv(self) -> pd.DataFrame:
        """Load CSV with multiple encoding attempts"""
        encodings = ['utf-8', 'latin-1', 'cp1252', 'utf-16']

        for encoding in encodings:
            try:
                df = pd.read_csv(self.file_path, encoding=encoding)
                print(f"✅ CSV loaded with {encoding} encoding")
                return df
            except:
                continue

        raise ValueError("Could not load CSV with any supported encoding")

class BaseDataCleaner(ABC):
    """Abstract base class for data cleaning operations"""

    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.cleaning_report = []

    @abstractmethod
    def clean(self) -> pd.DataFrame:
        """Abstract method for cleaning implementation"""
        pass

    def add_report(self, message: str):
        """Add operation to cleaning report"""
        self.cleaning_report.append(message)

class ColumnNameCleaner(BaseDataCleaner):
    """Clean and standardize column names"""

    def clean(self) -> pd.DataFrame:
        """Clean column names"""
        original_columns = self.df.columns.tolist()
        new_columns = []

        for col in self.df.columns:
            clean_col = str(col).strip()
            clean_col = re.sub(r'[^\w\s]', '', clean_col)
            clean_col = re.sub(r'\s+', '_', clean_col)
            clean_col = re.sub(r'_+', '_', clean_col)
            clean_col = clean_col.strip('_')
            new_columns.append(clean_col)

        self.df.columns = new_columns

        if original_columns != new_columns:
            self.add_report("Column names cleaned and standardized")
            print("✅ Column names cleaned")

        return self.df

class MissingValueHandler(BaseDataCleaner):
    """Handle missing values in dataset"""

    def clean(self) -> pd.DataFrame:
        """Handle missing values"""
        missing_before = self.df.isnull().sum().sum()

        if missing_before == 0:
            print("✅ No missing values found")
            return self.df

        columns_to_drop = []

        for column in self.df.columns:
            missing_count = self.df[column].isnull().sum()

            if missing_count == 0:
                continue

            missing_percentage = (missing_count / len(self.df)) * 100

            if missing_percentage > 50:
                columns_to_drop.append(column)
                self.add_report(f"Removed column '{column}' (>50% missing values)")
                continue

            self._fill_missing_values(column)

        # Drop columns with excessive missing values
        if columns_to_drop:
            self.df = self.df.drop(columns=columns_to_drop)

        missing_after = self.df.isnull().sum().sum()
        print(f"✅ Missing values handled: {missing_before} → {missing_after}")

        return self.df

    def _fill_missing_values(self, column: str):
        """Fill missing values based on column type"""
        try:
            if pd.api.types.is_numeric_dtype(self.df[column]):
                median_val = self.df[column].median()
                fill_value = median_val if not pd.isna(median_val) else 0
                self.df[column] = self.df[column].fillna(fill_value)
                self.add_report(f"'{column}' filled with median/zero")

            elif self.df[column].dtype == 'object':
                mode_val = self.df[column].mode()
                fill_value = mode_val[0] if not mode_val.empty else 'Unknown'
                self.df[column] = self.df[column].fillna(fill_value)
                self.add_report(f"'{column}' filled with mode/Unknown")

        except Exception as e:
            print(f"⚠️ Warning: Could not process column '{column}': {str(e)}")
            self.df[column] = self.df[column].fillna('Unknown')

class DuplicateRemover(BaseDataCleaner):
    """Remove duplicate rows from dataset"""

    def clean(self) -> pd.DataFrame:
        """Remove duplicate rows"""
        duplicates_before = self.df.duplicated().sum()

        if duplicates_before == 0:
            print("✅ No duplicate rows found")
            return self.df

        self.df = self.df.drop_duplicates()
        duplicates_after = self.df.duplicated().sum()

        removed = duplicates_before - duplicates_after
        self.add_report(f"Removed {removed} duplicate rows")
        print(f"✅ Duplicate rows removed: {removed}")

        return self.df

class TextDataCleaner(BaseDataCleaner):
    """Clean text data in dataset"""

    def clean(self) -> pd.DataFrame:
        """Clean text data"""
        text_columns = self.df.select_dtypes(include=['object']).columns

        for column in text_columns:
            try:
                self.df[column] = self.df[column].astype(str).str.strip()
                self.df[column] = self.df[column].str.replace(r'\s+', ' ', regex=True)
            except Exception as e:
                print(f"⚠️ Warning: Could not clean text in column '{column}': {str(e)}")

        self.add_report("Text data cleaned and standardized")
        print("✅ Text data cleaned")

        return self.df

class DataTypeOptimizer(BaseDataCleaner):
    """Optimize data types for better performance"""

    def clean(self) -> pd.DataFrame:
        """Optimize data types"""
        for column in self.df.columns:
            if self.df[column].dtype == 'object':
                self._optimize_object_column(column)

        self.add_report("Data types optimized")
        print("✅ Data types optimized")

        return self.df

    def _optimize_object_column(self, column: str):
        """Optimize object column data type"""
        try:
            sample_data = self.df[column].dropna().astype(str)

            if len(sample_data) == 0:
                return

            # Try datetime conversion
            if self._is_datetime_column(sample_data):
                self.df[column] = pd.to_datetime(self.df[column], errors='coerce')
                self.add_report(f"'{column}' converted to datetime")
                return

            # Try numeric conversion
            if self._is_numeric_column(sample_data):
                self.df[column] = pd.to_numeric(self.df[column], errors='coerce')
                self.add_report(f"'{column}' converted to numeric")

        except Exception as e:
            print(f"⚠️ Warning: Could not optimize column '{column}': {str(e)}")

    def _is_datetime_column(self, sample_data: pd.Series) -> bool:
        """Check if column contains datetime data"""
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',
            r'\d{2}/\d{2}/\d{4}',
            r'\d{2}-\d{2}-\d{4}',
        ]

        for pattern in date_patterns:
            if sample_data.str.match(pattern).any():
                return True
        return False

    def _is_numeric_column(self, sample_data: pd.Series) -> bool:
        """Check if column contains numeric data"""
        try:
            pd.to_numeric(sample_data.head(100), errors='raise')
            return True
        except:
            return False

class DataCleaner:
    """Main data cleaning orchestrator"""

    def __init__(self, file_path: str):
        self.file_path = file_path
        self.df = None
        self.original_df = None
        self.profile = None
        self.cleaning_report = []

    def load_data(self) -> bool:
        """Load data from file"""
        print("🔄 Loading data...")
        
        # Check system resources
        if not SystemResourceManager.check_system_resources():
            print("⚠️ Warning: Low system memory detected")
        
        # Estimate memory usage
        SystemResourceManager.estimate_memory_usage(self.file_path)
        
        # Load data
        loader = DataLoader(self.file_path)
        self.df = loader.load_data()
        
        if self.df is not None:
            self.original_df = self.df.copy()
            print(f"✅ Data loaded successfully: {self.df.shape[0]} rows, {self.df.shape[1]} columns")
            return True
        else:
            print("❌ Failed to load data")
            return False

    def profile_data(self) -> Dict:
        """Create comprehensive data profile"""
        if self.df is None:
            print("❌ No data loaded. Please load data first.")
            return {}

        print("🔍 Profiling data...")
        profiler = DataProfile(self.df)
        self.profile = profiler.create_profile()
        
        self._print_profile_summary()
        return self.profile

    def clean_all(self) -> pd.DataFrame:
        """Execute all cleaning operations"""
        if self.df is None:
            print("❌ No data loaded. Please load data first.")
            return pd.DataFrame()

        print("🧹 Starting data cleaning...")
        
        # Initialize cleaners
        cleaners = [
            ColumnNameCleaner(self.df),
            MissingValueHandler(self.df),
            DuplicateRemover(self.df),
            TextDataCleaner(self.df),
            DataTypeOptimizer(self.df)
        ]

        # Apply cleaning operations
        for cleaner in cleaners:
            self.df = cleaner.clean()
            self.cleaning_report.extend(cleaner.cleaning_report)

        print("✅ Data cleaning completed!")
        self._print_cleaning_summary()
        
        return self.df

    def save_cleaned_data(self, output_path: str = None) -> bool:
        """Save cleaned data to file"""
        if self.df is None:
            print("❌ No data to save. Please clean data first.")
            return False

        if output_path is None:
            # Generate output filename
            path = Path(self.file_path)
            output_path = path.parent / f"{path.stem}_cleaned{path.suffix}"

        try:
            if output_path.endswith('.xlsx'):
                self.df.to_excel(output_path, index=False)
            elif output_path.endswith('.csv'):
                self.df.to_csv(output_path, index=False)
            else:
                print("❌ Unsupported output format. Use .xlsx or .csv")
                return False

            print(f"✅ Cleaned data saved to: {output_path}")
            return True

        except Exception as e:
            print(f"❌ Error saving file: {str(e)}")
            return False

    def _print_profile_summary(self):
        """Print data profile summary"""
        if not self.profile:
            return

        print("\n📊 DATA PROFILE SUMMARY")
        print("=" * 50)
        
        # Basic info
        basic = self.profile['basic_info']
        print(f"📋 Dataset Size: {basic['total_rows']:,} rows × {basic['total_columns']} columns")
        print(f"💾 Memory Usage: {basic['memory_usage_mb']:.1f} MB")
        
        # Data quality
        quality = self.profile['data_quality']
        print(f"🔍 Data Quality:")
        print(f"   Missing Values: {quality['missing_values_total']:,}")
        print(f"   Duplicate Rows: {quality['duplicate_rows']:,}")
        print(f"   Completeness: {quality['completeness_percentage']:.1f}%")
        
        print("=" * 50)

    def _print_cleaning_summary(self):
        """Print cleaning operations summary"""
        print("\n🧹 CLEANING OPERATIONS SUMMARY")
        print("=" * 50)
        
        if self.cleaning_report:
            for i, operation in enumerate(self.cleaning_report, 1):
                print(f"{i}. {operation}")
        else:
            print("No cleaning operations were needed.")
        
        # Show before/after comparison
        if self.original_df is not None:
            print(f"\n📊 Before: {self.original_df.shape[0]:,} rows × {self.original_df.shape[1]} columns")
            print(f"📊 After:  {self.df.shape[0]:,} rows × {self.df.shape[1]} columns")
            
            rows_removed = self.original_df.shape[0] - self.df.shape[0]
            cols_removed = self.original_df.shape[1] - self.df.shape[1]
            
            if rows_removed > 0:
                print(f"🗑️  Removed {rows_removed:,} rows")
            if cols_removed > 0:
                print(f"🗑️  Removed {cols_removed} columns")
        
        print("=" * 50)

    def get_cleaning_report(self) -> List[str]:
        """Get detailed cleaning report"""
        return self.cleaning_report.copy()

    def reset_data(self):
        """Reset data to original state"""
        if self.original_df is not None:
            self.df = self.original_df.copy()
            self.cleaning_report = []
            print("✅ Data reset to original state")
        else:
            print("❌ No original data to reset to")

# Example usage
if __name__ == "__main__":
    # Example usage
    print("🔧 Professional Data Cleaner")
    print("=" * 30)
    
    # Initialize cleaner
    # cleaner = DataCleaner("your_file.xlsx")
    
    # Load and profile data
    # cleaner.load_data()
    # profile = cleaner.profile_data()
    
    # Clean data
    # cleaned_data = cleaner.clean_all()
    
    # Save cleaned data
    # cleaner.save_cleaned_data("cleaned_file.xlsx")
    
    print("Ready to use! Replace 'your_file.xlsx' with your actual file path.")