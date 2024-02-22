import streamlit as st
import pandas as pd
import PyPDF2
from PyPDF2 import PdfReader
from io import BytesIO
from langchain.text_splitter import RecursiveCharacterTextSplitter
from snowflake.snowpark import Session
from snowflake.snowpark.types import StringType, StructField, StructType
import config

class chunk_text:

    def process(self,text,chunk_size,chunk_overlap):
        text_raw=[]
        text_raw.append(text)

        text_splitter = RecursiveCharacterTextSplitter(
            separators = ["\n"], # Define an appropriate separator. New line is good typically!
            chunk_size = chunk_size,
            chunk_overlap  = chunk_overlap, #This let's text have some form of overlap. Useful for keeping chunks contextual
            length_function = len,
            add_start_index = True #Optional but useful if you'd like to feed the chunk before/after
        )

        chunks = text_splitter.create_documents(text_raw)
        # st.write(chunks)
        df = pd.DataFrame(chunks, columns=['chunks','metadata'])

        yield from df.itertuples(index=False, name=None)

def data_reset():
    if 'data_reset' in st.session_state:
        st.session_state.data_reset= True

def upload_file():
    uploaded_file = st.file_uploader("Choose a file", "pdf")
    return uploaded_file

def read_file(uploaded_file):  
    pdf_reader = PyPDF2.PdfReader(uploaded_file)
    text = ''

    for i in range(len(pdf_reader.pages)):
        page = pdf_reader.pages[i]
        text += page.extract_text()
    return text
  
def connect_snowflake():
    connection_parameters = {
        "account": config.account,
        "user": config.user,
        "password": config.password,
        "warehouse": config.warehouse,
        "database":config.database,
        "schema":config.schema}

    session = Session.builder.configs(connection_parameters).create()
    return session

def register_udf(session):
    schema = StructType([
    StructField("chunk", StringType()),
    StructField("meta", StringType()),
 ])

    session.udtf.register(
        handler = chunk_text,
        output_schema= schema,
        input_types = [StringType()] ,
        is_permanent = True ,
        name = 'CHUNK_TEXT' ,
        replace = True ,
        packages=['pandas','langchain'],
        stage_location = f"{config.database}.{config.schema}.PDF_STORE")

def create_table():
    
    return "hello"

def main():
    file = upload_file()
    if file is not None:
        st.success("Uploaded the file")
        data = read_file(file)
        chunk_sizes = [1000,1100,1200,1300]
        chunk_size = st.selectbox("Select Chunk_size", chunk_sizes)
        st.write(chunk_size)
        
        # df = pd.DataFrame([(file.name, data)], columns=["file_name", "raw_text"])
        # session = connect_snowflake()
        # snowpark_df3 = session.create_dataframe(df)
        # snowpark_df3.write.mode('overwrite').save_as_table('raw_text')
        # df = session.sql('select * from raw_text')
        # st.write(df)
        # register_udf(session)
        
        
    
if __name__ == '__main__':
    main()