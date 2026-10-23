WELCOME TO THE CS-E4780 FINANCIAL ANALYSIS APPLICATION PROJECT!

2024-2025 - Fall Semester

----------------------------------------------------------------

CONTRIBUTORS (A to Z):

- Elif Cemre Durgut (@elifcemredurgut)
- Metehan Koç (@DatAmstrongest)
- Tori Simon (@torisimon2)

----------------------------------------------------------------

HOW TO RUN THE PROJECT?

1) Place the .csv files in /producer/data

2) Run the project using the command below:
```
docker-compose up --build
```

3) Access the Grafana UI via the link below:

http://localhost:3000/

4) Enter the credentials:
```
username: admin

password: admin
```

5)  Click on "Financial Data Analysis" in the "Recently accessed dashbords" bar

6) Select one or more stock symbol(s) to view their price graph and breakouts

7) You can adjust the time range based on the data you have run.

8) The default refresh time is set to 5 seconds, but you can turn it on/off from the top-right corner.

9) For additional details regarding Apache Flink, the UI and logs are available here:

http://localhost:8081/

----------------------------------------------------------------

HOW TO TEST THE PROJECT?

1) Accuracy Test

1.1) Run the following Python file to get the EMAs and breakouts from the system:
```
cd accuracy-testing
pip install -r requirements.txt
python3 system_ema_breakout_collector.py
```

1.2) Run the following Python file to calculate the reference EMAs and breakouts. If the system has only replayed part of the data, pass `--until` with a trading time it has already passed:
```
python3 actual_ema_breakout_calculator.py --until "2021-11-08 15:30"
```

1.3) Run the following Python file to compare them. It only compares windows both sides have fully processed, and by default leaves out each stock's first-window bull breakout (both EMAs start at 0); add `--include-initial` to score those too:
```
python3 accuracy_checker.py
```

2) Latency Test

2.1) Connect to TimescaleDB container:
```
psql -U postgres -d stocksdb
```

2.2) Run the following SQL statement:
```
select AVG(latency_end - latency_start)  from breakouts;
```

----------------------------------------------------------------

PROJECT DOCUMENTS

Project Demo Video:

https://youtu.be/DMKm5llYghA
