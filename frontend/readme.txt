Project Name: Data Quality Analyzer

Requirements:
- Python 3.9
- pip

Steps to run:

1. Clone the repository:
git clone https://github.com/hema-1506/data-quality-app.git

2. Navigate to folder:
cd data-quality-app

3. Create virtual environment:
python -m venv venv

4. Activate environment:
source venv/bin/activate

5. Install dependencies:
pip install -r requirements.txt

6. Run application:
gunicorn app:app --bind 0.0.0.0:8000

7. Open in browser:
http://localhost:8000