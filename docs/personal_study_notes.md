# MY FIRST DATA SCIENCE PROJECT: EXTENDED STUDY GUIDE

**The Big Goal:** Data Science isn't about doing complicated math just for the sake of it. It's about taking a business "hunch", looking for real-world proof, and showing with numbers whether that hunch is true or false to save (or make) money.

In my case, the boss (the Hotel Director) had a hunch that Calima deterred last-minute bookings. My job was to build a "data pipeline" to investigate this.

A data pipeline is exactly like cooking: you buy the ingredients, wash them, cook them, plate them, and eat. Let's look at each step:

---

## 1. BUYING THE INGREDIENTS (Data Extraction)

You can't cook without ingredients. I needed information from two different worlds: tourists and the weather.

* **Tourists (Downloading CSV Files):** I used ISTAC "Microdata".
  * *The concept:* Normally, public reports give you the summary ("1,000 people came"). Microdata is like **individual cash register receipts**. By having the receipt for each tourist, I was able to filter and keep only the people I cared about: those who went to 4-star hotels in Gran Canaria.
* **The Weather (Connecting to an API):** I used the AEMET API.
  * *The concept:* An API is like **a waiter in a restaurant**. You don't go into the AEMET kitchen to get the data yourself. You give them your "API Key" (your membership card to prove who you are), you place your order ("give me the weather for 2025"), and they bring you the data on a tray.
  * *The Rate Limit problem:* I asked for a whole year and got an error. It's like asking a cook to make 365 pizzas at once; they get overwhelmed. I had to use **"Batching"**: ask for 6 months, wait 2 seconds, and ask for the other 6 months.
* **The "Granularity" problem:** AEMET spoke in "Days" and ISTAC spoke in "Months". Since you can't mix apples and oranges, I had to decide to "compress" the AEMET days and add them up by months so both tables spoke the same language.

---

## 2. WASHING AND CUTTING INGREDIENTS (Data Cleaning)

Computers are powerful, but they are very "dumb". If a piece of data comes in an unexpected format, the program breaks. 80% of an analyst's time is spent here.

* **The text vs. numbers problem (Type Casting):** In Europe, decimals are often written with a comma ("18,2"). For Python, which is made in English, numbers have a dot ("18.2"). If Python sees a comma, it thinks it's a word. If I try to calculate the average temperature by adding "words", the code explodes. I had to program a translator that changed commas to dots.
* **Ghost Data (Null Values / NaN):** When I went to count tourists in summer, I got "0", but I knew there were people. What happened?
  * *The analogy:* Imagine a classroom with 30 students. You ask them to write down their Name and ID on a piece of paper. Everyone puts their name, but halfway through the year they stop putting their ID. If you tell the computer "count how many IDs there are", it will tell you "0". If you tell it "count how many students are sitting (how many rows)", it will tell you "30".
  * *The lesson:* File formats change without warning. Never trust a single column. I learned to use a robust count (counting entire rows instead of specific boxes).

---

## 3. COOKING THE RECIPE (Feature Engineering)

Here I showed I could think like a true scientist. AEMET gave me temperature, wind, and humidity, but **didn't give me any column that said "Calima Day"**. I had to invent it.

* **The Proxy Variable:** This is using data I *do* have to guess data I *don't* have. I know Calima happens when it's very hot and humidity drops suddenly.
* **Attempt 1 - The rigid rule (Static Threshold):** I said: "If it's over 27.5ºC, it's Calima". The code told me that in August there were 20 days of Calima. I realized that was a lie.
  * *Why it failed:* Using a fixed rule ignores context. Wearing a coat is fine if it's 10ºC, but if you go to the Caribbean, 10ºC is the end of the world. In August, 28ºC is normal, not Calima.
* **Attempt 2 - The smart model (Dynamic Threshold):** I taught Python to calculate what was "normal" for each specific month.
  * *The concept:* The code looked at August, saw that normal was 28ºC, and said: "Okay, I'll only trigger the Calima alarm if a day reaches 33ºC (+4.5ºC above normal) and on top of that there's no humidity". This is how I managed to detect real anomalies (weird spikes) and eliminate false positives (normal summer days that the model thought were Calima).

---

## 4. PLATING (Data Merging / Join)

I had two separate Excel tables: Weather and Tourists. I had to join them.

* **The Glue (Left Join):** To join two tables you need a "key" that both share. My key was the **Month (WAVE)** column. It's like a zipper: month 1 of the weather fits with month 1 of the tourists; month 8 fits with month 8, etc.

## 5. TASTING (Statistical Analysis)

I had everything in one table, I just had to see if there was a relationship between Calima days and tourists booking late.

* **Pearson Correlation:** This is a mathematical formula that compares two columns and gives you a number from -1 to 1.
  * **+1 (Direct):** When one goes up, the other goes up too. (Ex: Ice cream sales and temperature).
  * **-1 (Inverse):** When one goes up, the other goes down. (Ex: Scarf sales and temperature). The Director thought I would be here: "More Calima, fewer tourists".
  * **0 (Independence):** Things have nothing to do with each other.
* **My result was 0.268.** Basically a very weak relationship. Mathematically I proved that bookings go up and down for other reasons (prices, school holidays, flights), but the tourist who books late doesn't care at all if the sky is orange or blue.

---

## 6. THE IMPACT (Making "Data-Driven" decisions)

In companies there is something called the "HiPPO" (*Highest Paid Person's Opinion*). Normally, what the boss says is done because "they have experience".

I took the boss's opinion and confronted it with the data (I became *Data-Driven*).

**The great triumph of this project:** Discovering that *nothing happens* is an incredible discovery. By knowing that Calima doesn't scare customers away, I saved the hotel from spending money on emergency advertising or dropping prices out of fear. I proved that with a laptop and free public data, I can optimize the profits of a whole company.
