import argparse
import os
from pipeline_Script import PandasETLPipeline
from logger import setupLogger

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",required=True)
    parser.add_argument("--output",required=True,default="output.csv")
    parser.add_argument("--min_spend",type=int,required=True)

    args = parser.parse_args()

    root_dir = os.path.dirname(os.path.abspath(__file__))
    input_path = args.input if os.path.isabs(args.input) else os.path.join(root_dir,args.input)
    output_path = args.output if os.path.isabs(args.output) else os.path.join(root_dir,args.output)

    log_file = os.path.join(root_dir,"pipeline.log")
    setupLogger(log_file)

    pipeline = PandasETLPipeline(
        input_path=input_path,
        output_path=output_path,
        min_spend=args.min_spend
    )

    pipeline.run_pipeline()

if __name__ == "__main__":
    main()