import csv
import random
from faker import Faker
import sys
import os

fake = Faker()

def generate_person():
    """
    Generates a single dictionary representing one employee.
    This keeps the logic for 'what is a person' isolated.
    """
    first_name = fake.first_name()
    last_name = fake.last_name()
    salary = random.randint(30000, 120000)
    department = random.choice(['HR', 'Engineering', 'Sales', 'Marketing'])

    # Adding dirty data
    if random.random() < 0.1:
        first_name = f' {first_name}' 
    if random.random() < 0.1:
        first_name = f'{first_name} '

    if random.random() < 0.1:
        last_name = f' {last_name}' 
    if random.random() < 0.1:
        last_name = f'{last_name} '

    if random.random() < 0.1:
        salary = random.choice([f'${salary}', f'$ {salary}', f'{salary} $'])


    if random.random() < 0.1:
        department = department.lower()

    if random.random() < 0.01:
        salary = f'-{salary}'
    elif random.random() < 0.01:
        salary = 9999999

    email = f"{first_name.strip().lower()}.{last_name.lower()}@company.com"
    if random.random() < 0.05:
        email = ""

    return {
        "id": fake.unique.random_int(min=111111, max=999999),
        "first_name": first_name,
        "last_name": last_name,
        "email": email,
        "salary": salary,
        "department": department,
        "hire_date": fake.date_between(start_date='-10y', end_date='today')
    }


def save_to_csv(num_rows, output_path):
    """
    Main loop that calls generate_person() N times and writes to CSV.
    """
    with open(output_path, 'w', newline='') as f:
        fieldnames = ["id", "first_name", "last_name", "email", "salary", "department", "hire_date"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for _ in range(num_rows):
            person_data = generate_person()
            writer.writerow(person_data)


if __name__ == "__main__":
    num_rows = 100
    output_path = "hr_data.csv"

    if len(sys.argv) > 2:
        num_rows = int(sys.argv[1])
        output_path = sys.argv[2]
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    print(f"Generating {num_rows} rows to {output_path}...")
    save_to_csv(num_rows, output_path)
    print(f"Success! File created at {output_path}")