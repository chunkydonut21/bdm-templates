N_LINES := 	$(if $(N_LINES),$(N_LINES),100000)

run:
	docker compose up

gen-sample:
	mkdir src/sample_$(N_LINES) \
	&& head -n $(N_LINES) src/trip_data/trip_data_1.csv >> src/sample_$(N_LINES)/sample.csv
	