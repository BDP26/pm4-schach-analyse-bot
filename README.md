# pm4-schach-analyse-bot

Schach-Analyse-Bot mit Daten der Lichess Open Database, für das Modul PM4

Contributors: Nelly Mossig, Chris Eggenberger



\-schritte zur performance Steigerung im ETL prozess: 

inhaltlich:

Early Stopping Begrenzung auf die ersten 40 Züge jedes Spiels, da späte Endspiele für ein Dashboard eher irrelevant sind.

Data Quality Filtering: Ausschluss von Bullet-Partien, abgebrochenen Spielen und Spielen unter einem gewissen Elo-Niveau direkt beim Einlesen.

Elo Bucketing: Runden der Elo-Zahlen auf 100er-Schritte, um die Daten später überhaupt sinnvoll gruppieren zu können.

Pruning des "Long Tails": Wegen der exponentiellen Komplexität von Schach explodierte die Anzahl einzigartiger Brettpositionen im Mittelspiel. Durch das Verwerfen von Zügen, die weniger als 3 Mal gespielt wurden, wurde die finale Datenbank extrem verkleinert.



computing:

Parallel Processing: Verteilung der gefilterten Partien auf die 16 Cores deines Desktop-PCs.

Chunking: Sammeln von 10 Millionen Zügen im schnellen Arbeitsspeicher (RAM), bevor sie auf die Festplatte geschrieben werden. Passender Tradeoff zwischen RAM-Nutzung und Speicherkomplexität.

Technologie-Evaluation: Testen von PyPy als Alternative zu Python (wurde wegen Inkompatibilitäten/Performance-Trade-offs verworfen)



Storage \& Aggregation

Columnar Storage (Parquet vs. CSV): Speicherung der Chunks im spaltenbasierten .parquet-Format. Das sorgte für massive Datenkompression und erlaubte es DuckDB später, blitzschnell nur die relevanten Spalten zu lesen.

Horizontale Partitionierung (Divide \& Conquer): Um Out of Memory-Crashes bei der finalen Aggregation zu verhindern, wurden die Daten nicht als Ganzes, sondern isoliert pro Elo-Bucket (Schleife) aggregiert.



Runtime von mehreren Wochen auf 9 Stunden auf meinem Desktop PC (64 GB Ram, 16 Cores)



