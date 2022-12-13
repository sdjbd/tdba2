import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import matplotlib as plt
from datetime import datetime
import streamlit as st
import plotly.express as px






def createEngine():
    engine = 'postgresql://lectura:ncorrea#2022@138.100.82.178:5432/2207'
    return engine


def dateToMilisec_input(date):
    time = datetime.strptime(str(date), 
                            '%Y-%m-%d %H:%M:%S')
    return time.timestamp()*1000    

def dateToMilisec(date):
    time = datetime.strptime(str(date), 
                            '%d/%m/%Y %H:%M:%S')
    return time.timestamp()*1000                    



#Define a start and end time that returns every start and end of programs
def operatingPeriods(start, end):

    #we transform the start and en date into milliseconds:
    start = dateToMilisec_input(start)

    end = dateToMilisec_input(end)
    eng_string=createEngine()
    engine = create_engine(eng_string)


    #We then request the data base to print the variables in the selected time frame
    Request="select TO_CHAR(TO_TIMESTAMP(date / 1000), 'DD/MM/YYYY HH24:MI:SS') AS date,name,value from variable_log_float\
         left join variable on variable_log_float.id_var = variable.id where date > "+str(start)+" and date < "+str(end)+" and\
            id_var=597 order by date"

    Program_list=pd.read_sql_query(Request, con = engine)
    
    #When a program starts Program_run goes to 1 and finished to 0, the opposite when finished or interupted

    op=Program_list.to_numpy() #we transform the df to an array, as it is easier to read
    i=0
    if op[0,2] == 0:
        i=1

    periods = []
    i=0
    j=1
    while i < len(op)-1:
        periods.append(['OP'+str(j),op[i,0],op[i+1,0]]) #we name the operating time values (OP1, OP2)
        i=i+2
        j=j+1

    periods_df = pd.DataFrame(periods, columns = ['operating period','start','end'])
    
    return periods_df


def insideOP(op, periods):

    periods_df = periods
    eng_string=createEngine()
    engine = create_engine(eng_string)

    start_day = periods_df.loc[periods_df['operating period'] == op, 'start'].iloc[0]
    end_day = periods_df.loc[periods_df['operating period'] == op, 'end'].iloc[0]

    start = dateToMilisec(start_day)
    end = dateToMilisec(end_day)


    Request="select TO_CHAR(TO_TIMESTAMP(date / 1000), 'DD/MM/YYYY HH24:MI:SS') AS date, id_var, value, name from variable_log_string\
        left join variable on variable_log_string.id_var = variable.id where date >= "+str(start)+" and date <= "+str(end)+" order by date\
            limit 20"
    actions=pd.read_sql_query(Request, con = engine)

    return actions


def energyTemp(op, periods):

    periods_df = periods
    eng_string=createEngine()
    engine = create_engine(eng_string)

    start_day = periods_df.loc[periods_df['operating period'] == op, 'start'].iloc[0]
    end_day = periods_df.loc[periods_df['operating period'] == op, 'end'].iloc[0]

    start = dateToMilisec(start_day)
    end = dateToMilisec(end_day)

    Request="select TO_CHAR(TO_TIMESTAMP(date / 1000), 'DD/MM/YYYY HH24:MI:SS') AS date, id_var, value, name from variable_log_float\
    left join variable on variable_log_float.id_var = variable.id where date >= "+str(start)+" and date <= "+str(end)+" and name like '%%TEMP%%' order by date\
        limit 20"
    df=pd.read_sql_query(Request, con = engine)

    #Aggregate table by variable mesured into a list of value 
    temp_evolution = df.groupby(['name'], as_index=False).agg({'date': list,'value': list})

    return temp_evolution


def autoManual(start, end):

    
    #we transform the start and en date into milliseconds:
    start = dateToMilisec_input(start)
    end = dateToMilisec_input(end)
    eng_string=createEngine()
    engine = create_engine(eng_string)


    Request = "select id_var, TO_CHAR(TO_TIMESTAMP(date / 1000), 'DD/MM/YYYY HH24:MI:SS') AS date,value from\
         variable_log_float where date > "+str(start)+" and date < "+str(end)+" and\
            id_var=622 order by date limit 25"

    t=pd.read_sql_query(Request, con = engine)

    t['mode'] = np.where(t['value']<2, 'automatic', 'manual')
    t = t.loc[t["mode"].shift() != t["mode"]]#we delete the rows that have a dupplicate name
    t.reset_index(drop=True,inplace=True)

    auto_manual=t.to_numpy() #we transform the df to an array, as it is easier to read

    periods_auto_manual_5 = []
    i=0
    j=1
    while i < len(auto_manual)-1:
        periods_auto_manual_5.append(['A'+str(j),auto_manual[i,3],auto_manual[i,1],auto_manual[i+1,1]])
        i=i+1
        j=j+1

    periods_auto_manual_df = pd.DataFrame(periods_auto_manual_5, columns = ['name','mode','start','end'])

    return periods_auto_manual_df



def maOperations(start, end):
    
    eng_string=createEngine()
    engine = create_engine(eng_string)
    periodsAM = autoManual(start,end)

    start = dateToMilisec_input(start)
    end = dateToMilisec_input(end)
    operations = pd.DataFrame()

    i=0
    while i<len(periodsAM):
        s = periodsAM['start'][i]
        e = periodsAM['end'][i]
        s = dateToMilisec(s)
        e = dateToMilisec(e)

        Request="select TO_CHAR(TO_TIMESTAMP(date / 1000), 'DD/MM/YYYY HH24:MI:SS') AS date, id_var, value, name from variable_log_string\
        left join variable on variable_log_string.id_var = variable.id where date >= "+str(s)+" and date <= "+str(e)+" order by date"
        a=pd.read_sql_query(Request, con = engine)
        a["mode"] = periodsAM['mode'][i]
        operations = operations.append(a)
        i=i+1


    operations2 = operations.groupby('mode').agg({'value':'count'})

    time_min = (end-start)/1000/60
    opPerMin = (operations2['value'].sum())/time_min

    return operations2, opPerMin




def energyTempAutomatic(periodsAM,Name):

    eng_string=createEngine()
    engine = create_engine(eng_string)
    periodsAM=periodsAM

    start_day = periodsAM.loc[periodsAM['name'] == Name, 'start'].iloc[0]
    end_day = periodsAM.loc[periodsAM['name'] == Name, 'end'].iloc[0]

    #start = dateToMilisec(start_day)
    #end = dateToMilisec(end_day)


    Request="select TO_CHAR(TO_TIMESTAMP(date / 1000), 'DD/MM/YYYY HH24:MI:SS') AS date, id_var, value, name from variable_log_float\
        left join variable on variable_log_float.id_var = variable.id where date >= "+str(start_day)+" and date <= "+str(end_day)+" and id_var=735 order by date"
    c=pd.read_sql_query(Request, con = engine)

    temp_evolution = c.groupby(['name'], as_index=False).agg({'date': list,'value': list})
  
    return temp_evolution

def gantt_1(start, end):

    start = dateToMilisec_input(start)
    end = dateToMilisec_input(end)
    eng_string=createEngine()
    engine = create_engine(eng_string)

    R = "select id_var, TO_CHAR(TO_TIMESTAMP(date / 1000), 'DD/MM/YYYY HH24:MI:SS') AS date,value from variable_log_float where date >= "+str(start)+" and date <= "+str(end)+" and id_var=622 order by date"

    t=pd.read_sql_query(R, con = engine)

    t['mode'] = np.where(t['value']<2, 'automatic', 'manual')
    t = t.loc[t["mode"].shift() != t["mode"]]#we delete the rows that have a dupplicate name
    t.reset_index(drop=True,inplace=True)

    auto_manual=t.to_numpy() #we transform the df to an array, as it is easier to read

    periods_auto_manual = []
    i=0
    while i < len(auto_manual)-1:
        periods_auto_manual.append([auto_manual[i,3],auto_manual[i,1],auto_manual[i+1,1]])
        i=i+1


    periods_auto_manual_df = pd.DataFrame(periods_auto_manual, columns = ['mode','start','end'])

    Starts=[datetime.strptime(periods_auto_manual_df["start"][i], "%d/%m/%Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S") for i in range(len(periods_auto_manual_df["start"]))]
    Endings=[datetime.strptime(periods_auto_manual_df["end"][i], "%d/%m/%Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S") for i in range(len(periods_auto_manual_df["end"]))]



    #MODE GANTT
    

    Starts_modes=[datetime.strptime(periods_auto_manual_df["start"][i], "%d/%m/%Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S") for i in range(len(periods_auto_manual_df["start"]))]
    Endings_modes=[datetime.strptime(periods_auto_manual_df["end"][i], "%d/%m/%Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S") for i in range(len(periods_auto_manual_df["end"]))]


    df2 = pd.DataFrame([dict(Task="Mode"+str(i), Start=Starts_modes[i], Finish=Endings_modes[i],mode=periods_auto_manual_df["mode"][i]) for i in range(len(Starts_modes))]) 

    
    fig = px.timeline(df2, x_start="Start", x_end="Finish", y="Task",color="mode")
    fig.update_yaxes(autorange="reversed") # otherwise tasks are listed from the bottom up
    #fig.show()

    return fig
    

    

def gantt_2(start, end):


    # JOINT TIME AND MODE GANTT 
    joint_df=operatingPeriods(start,end)

    start = dateToMilisec_input(start)
    end = dateToMilisec_input(end)
    eng_string=createEngine()
    engine = create_engine(eng_string)

    R = "select id_var, TO_CHAR(TO_TIMESTAMP(date / 1000), 'DD/MM/YYYY HH24:MI:SS') AS date,value from variable_log_float where date >= "+str(start)+" and date <= "+str(end)+" and id_var=622 order by date"

    t=pd.read_sql_query(R, con = engine)

    t['mode'] = np.where(t['value']<2, 'automatic', 'manual')
    t = t.loc[t["mode"].shift() != t["mode"]]#we delete the rows that have a dupplicate name
    t.reset_index(drop=True,inplace=True)

    auto_manual=t.to_numpy() #we transform the df to an array, as it is easier to read

    periods_auto_manual = []
    i=0
    while i < len(auto_manual)-1:
        periods_auto_manual.append([auto_manual[i,3],auto_manual[i,1],auto_manual[i+1,1]])
        i=i+1


    periods_auto_manual_df = pd.DataFrame(periods_auto_manual, columns = ['mode','start','end'])

    Starts=[datetime.strptime(periods_auto_manual_df["start"][i], "%d/%m/%Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S") for i in range(len(periods_auto_manual_df["start"]))]
    Endings=[datetime.strptime(periods_auto_manual_df["end"][i], "%d/%m/%Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S") for i in range(len(periods_auto_manual_df["end"]))]


    

    joint_df['mode']=""
    for i in range (len(joint_df)):
        for j in range(len(periods_auto_manual_df)):
            if(joint_df["start"][i]>=periods_auto_manual_df["start"][j]):
                mode_start=j
            if(joint_df["end"][i]<=periods_auto_manual_df["end"][j] and j==0): 
                mode_end=j
            elif(joint_df["end"][i]<=periods_auto_manual_df["end"][j] and joint_df["end"][i]>periods_auto_manual_df["end"][j-1]):
                mode_end=j
        if(mode_start==mode_end):
            joint_df["mode"][i]=periods_auto_manual_df["mode"][mode_start]
        else:
            real_end=joint_df["end"][i]
            joint_df["end"][i]=periods_auto_manual_df["end"][mode_start]
            joint_df["mode"][i]=periods_auto_manual_df["mode"][mode_start]
            line = pd.DataFrame({"operating period": joint_df["operating period"][i], "start":joint_df["end"][i],"end":real_end,"mode":""}, index=[3])
            joint_df = pd.concat([joint_df.iloc[:i+1], line, joint_df.iloc[i+1:]]).reset_index(drop=True)

    import plotly.figure_factory as ff
    colors = {'automatic': 'rgb(0, 255, 255)',
            'manual': "rgb(0,0,255)"}

    Starts_joint=[datetime.strptime(joint_df["start"][i], "%d/%m/%Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S") for i in range(len(joint_df["start"]))]
    Endings_joint=[datetime.strptime(joint_df["end"][i], "%d/%m/%Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S") for i in range(len(joint_df["end"]))]

    df3 = pd.DataFrame([ dict(Task=joint_df["operating period"][i], Start=Starts_joint[i], Finish=Endings_joint[i], mode=joint_df["mode"][i]) for i in range(len(joint_df))])
    fig = px.timeline(df3, x_start="Start", x_end="Finish", y="Task",color="mode")
    fig.update_yaxes(autorange="reversed") # otherwise tasks are listed from the bottom up
    
    return fig

#pipenv shell
#streamlit run .py


st.title('TBDA')

with st.sidebar:
    with st.form("my_form"):
        st.title("Input dates")
        #start = st.text_input('START (00/00/0000 00:00:00)')
        #end = st.text_input('END (00/00/0000 00:00:00)')

        start_date = str(st.date_input(label="Start date", label_visibility="visible"))
        start_time = str(st.time_input(label="Start time", label_visibility="visible"))

        end_date = str(st.date_input(label="End date", label_visibility="visible"))
        end_time = str(st.time_input(label="End time", label_visibility="visible"))

        start = start_date + " " + start_time
        end = end_date + " " + end_time
       
    
        submitted = st.form_submit_button("Submit")

        if submitted:
        
            if 'start' not in st.session_state:
                st.session_state['start'] = start

            if 'end' not in st.session_state:
                st.session_state['end'] = end

        

tab1, tab2, tab3 = st.tabs(["Home", "Operating Periods", "Automatic & Manual"])

engine = create_engine('postgresql://nlectura:correa@138.100.82.184:5432/2207')

@st.cache
def load_op(start, end):
    data = operatingPeriods(start, end)
    return data
@st.cache
def inside_op(period, periods):
    data = insideOP(period, periods)
    return data
@st.cache
def energy(period, periods):
    data = energyTemp(period, periods)
    return data
@st.cache
def auto_manual(start, end):
    data = autoManual(start, end)
    return data
@st.cache
def AMop(start, end):
    data = maOperations(start, end)
    return data

with tab1:

    st.markdown("""
                    <style>
                    .big-font {
                        font-size:75px !important;
                    }
                    </style>
                    """, unsafe_allow_html=True)

    st.markdown('<p class="big-font">Welcome to the Team 1 web app!</p>', unsafe_allow_html=True)

    

with tab2:

    if st.checkbox('Show Operating Periods'):
        st.subheader('Operating periods')
        op = load_op(start, end)
        st.write(op)
        
        op=load_op(start, end)

        selectedPeriod = st.selectbox('Select OP', op)

        if st.checkbox('Show Actions and Energy'):
        

            if selectedPeriod!='':
                st.subheader('Actions')
                actions = inside_op(selectedPeriod, op)
                st.write(actions)


            if selectedPeriod!='':
                st.subheader('Energy')
                energy = energy(selectedPeriod, op)
                st.write(energy)

                select = st.selectbox('select variable', energy["name"])
                if st.checkbox('Show energy cons'):
                    
                    dates = [x for x in energy.loc[energy['name'] == select, 'date']]
                    values = [x for x in energy.loc[energy['name'] == select, 'value']]
                    dates = [j for sub in dates for j in sub]
                    values = [j for sub in values for j in sub]
                    #dates  = [item for dates in l for item in dates]
                    #values  = [item for values in l for item in values]
                    data_plot = list(zip(dates,values))
                    data_plot_df = pd.DataFrame(data_plot, columns =['date', 'value'])
                    st.line_chart(data_plot_df, x='date', y='value')

with tab3:                   
    
    if st.checkbox('Automatic and Manual periods'):

        st.subheader('Automatic & Manual modes over time')
        fig = gantt_1(start, end)
        st.plotly_chart(fig)

        st.subheader('Automatic and Manual periods')
        periodos = auto_manual(start, end)
        st.write(periodos)


    if st.checkbox('Automatic vs Manual operations'):

        st.subheader('Operations manual vs automatic')
        (operations, operationsMin) = AMop(start, end)

        st.subheader('Operations over time')
        fig2 = gantt_2(start, end)
        st.plotly_chart(fig2)
        
        st.bar_chart(operations)
        st.text(str(round(operationsMin,2))+ ' Operations per minute.')
