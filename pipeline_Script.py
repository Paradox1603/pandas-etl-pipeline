import pandas as pd
import logging

class PandasETLPipeline:
    def __init__(self, input_path: str, output_path: str, min_spend: int):
        self.input_path = input_path
        self.output_path = output_path
        self.result = None
        self.min_spend = min_spend
        self.logger = logging.getLogger("pipeline")
    
    def read_data(self) -> pd.DataFrame:
        self.logger.info("Getting data from the JSON")
        df = pd.read_json(self.input_path)
        return df

    def transformation(self,df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info("Starting the transformation")
        self.logger.info(f"Rows before Transformation: {len(df)}")

        invalid_name = df[df["name"].str.strip() == ""]
        invalid_nulls = df[df[["name","amount"]].isnull().any(axis=1)]

        if not invalid_name.empty:
            self.logger.warning(f"Dropped rows (empty name): {invalid_name.to_dict(orient='records')}")
        if not invalid_nulls.empty:
            self.logger.warning(f"Dropped rows (empty name and amount): {invalid_nulls.to_dict(orient='records')}")

        #Cleaning the data
        df = df[df["name"].str.strip() != ""]
        df = df.dropna(subset=["name","amount"])

        #Group and Aggregation
        grouped = (
            df.groupby(["customer_id","name"],as_index=False)
            .agg(total_spend=("amount","sum"))
        )

        #HAVING- Filter post Aggregation
        self.result = grouped[grouped["total_spend"] > self.min_spend]
        self.logger.info(f"Row after Transformation: {len(self.result)}")

        return self.result
    
    def load_data(self, df: pd.DataFrame) -> None:
        self.logger.info("Going to save the Transformed Data")
        df.to_csv(self.output_path,index=False)
    
    def run_pipeline(self) -> None:
        self.logger.info("Starting the Pipeline")

        df = self.read_data()
        transformed = self.transformation(df)
        self.load_data(transformed)

        self.logger.info("Pipeline Executed Successfully")