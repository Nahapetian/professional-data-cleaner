"""
Simple example of using Professional Data Cleaner
"""

import pandas as pd
from data_cleaner import DataCleaner

def create_sample_data():
    """Create sample data for testing"""
    sample_data = pd.DataFrame({
        'Name': ['John Doe', 'Jane Smith', None, 'Bob Johnson', 'John Doe'],
        'Age': [25, None, 30, 25, 25],
        'City': ['New York', 'Los Angeles', 'Chicago', 'New York', 'New York'],
        'Salary': ['50000', '75000', None, '60000', '50000'],
        'Email': ['john@email.com', 'jane@email.com', 'bob@email.com', None, 'john@email.com'],
        'Department': ['IT', 'Marketing', 'Sales', 'IT', 'IT']
    })
    
    # Save sample data
    sample_data.to_excel('sample_data.xlsx', index=False)
    print("✅ Sample data created: sample_data.xlsx")
    return sample_data

def main():
    """Main example function"""
    print("🔧 Professional Data Cleaner - Example Usage")
    print("=" * 50)
    
    # Step 1: Create sample data
    print("\n1️⃣ Creating sample data...")
    sample_data = create_sample_data()
    print(f"   Original data: {sample_data.shape[0]} rows, {sample_data.shape[1]} columns")
    
    # Step 2: Initialize cleaner
    print("\n2️⃣ Initializing Data Cleaner...")
    cleaner = DataCleaner('sample_data.xlsx')
    
    # Step 3: Load data
    print("\n3️⃣ Loading data...")
    if cleaner.load_data():
        print("   Data loaded successfully!")
    else:
        print("   Failed to load data!")
        return
    
    # Step 4: Profile data
    print("\n4️⃣ Profiling data...")
    profile = cleaner.profile_data()
    
    # Step 5: Clean data
    print("\n5️⃣ Cleaning data...")
    cleaned_data = cleaner.clean_all()
    
    # Step 6: Save cleaned data
    print("\n6️⃣ Saving cleaned data...")
    if cleaner.save_cleaned_data('cleaned_sample_data.xlsx'):
        print("   Cleaned data saved successfully!")
    
    # Step 7: Show cleaning report
    print("\n7️⃣ Cleaning Report:")
    report = cleaner.get_cleaning_report()
    for i, operation in enumerate(report, 1):
        print(f"   {i}. {operation}")
    
    print("\n🎉 Example completed successfully!")
    print("📁 Files created:")
    print("   • sample_data.xlsx (original)")
    print("   • cleaned_sample_data.xlsx (cleaned)")

if __name__ == "__main__":
    main()