📊 GitHub Dependents Analysis
Analyze public dependent repositories of a given GitHub repository, categorize them by star count, and visualize the results.
🚀 Overview
This project uses the github-dependents-info CLI tool to:
Fetch public dependent repositories of any GitHub repository.
Extract repository name and stars.
Categorize repositories based on star ranges.
Output results in both table format and graphical visualization.
Example use case:
"How popular are projects that depend on tslearn-team/tslearn?"
🛠 Features
📥 Fetch dependents for any GitHub repository.
📊 Categorize into:
Below 100 stars
100 to 1000 stars
1000+ stars
📈 Bar chart visualization for better insights.
💾 Pandas DataFrame output for further processing.
📂 Project Structure
📦 github-dependents-analysis
 ┣ 📜 main.py         # Main script
 ┣ 📜 requirements.txt # Dependencies
 ┗ 📜 README.md       # Documentation
🔧 Installation
1️⃣ Clone the Repository
git clone https://github.com/your-username/github-dependents-analysis.git
cd github-dependents-analysis
2️⃣ Install Dependencies
pip install -r requirements.txt
3️⃣ Install github-dependents-info CLI Tool
npm install -g github-dependents-info
▶️ Usage
Run the script:
python main.py
Example Output (Table)
       Star Range   Library Count
0  Below 100 stars           240
1  100 to 1000 stars          32
2  1000+ stars                5
Example Output (Bar Graph)
The script generates a bar chart:
X-axis: Star Ranges
Y-axis: Number of Libraries
📜 Code Example
import subprocess
import json
import pandas as pd
import matplotlib.pyplot as plt

result = subprocess.run(
    ["github-dependents-info", "--repo", "tslearn-team/tslearn", "--json"],
    capture_output=True,
    text=True
)

data = json.loads(result.stdout)
df = pd.DataFrame(data["all_public_dependent_repos"])[["name", "stars"]]
df_sorted = df.sort_values(by="stars")

counts = {
    "Below 100 stars": ((df_sorted["stars"] < 100)).sum(),
    "100 to 1000 stars": ((df_sorted["stars"] >= 100) & (df_sorted["stars"] <= 1000)).sum(),
    "1000+ stars": (df_sorted["stars"] > 1000).sum()
}

final_df = pd.DataFrame(list(counts.items()), columns=["Star Range", "Library Count"])
print(final_df)

# Visualization
plt.bar(final_df["Star Range"], final_df["Library Count"], color="skyblue")
plt.xlabel("Star Range")
plt.ylabel("Number of Libraries")
plt.title("Dependent Repositories by Star Range")
plt.show()
📌 Notes
You must have Node.js installed for github-dependents-info.
Large repositories with thousands of dependents may take longer to fetch.
This script works for public repositories only.