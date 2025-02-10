'''
Напишите систему алертов для приложения
Система должна с периодичность каждые 15 минут проверять ключевые метрики — такие как активные пользователи в ленте / мессенджере, просмотры, лайки, CTR, количество отправленных сообщений. 

В случае обнаружения аномального значения, в чат должен отправиться алерт — сообщение со следующей информацией: метрика, ее значение, величина отклонения.

В сообщение можно добавить дополнительную информацию, которая поможет при исследовании причин возникновения аномалии. Это может быть, например, график. 

Пример шаблона алерта: 

Метрика {metric_name} в срезе {group}. 
Текущее значение {current_x}. Отклонение более {x}%.
[опционально: ссылка на риалтайм чарт этой метрики в BI для более гибкого просмотра]
[опционально: ссылка на риалтайм дашборд в BI для исследования ситуации в целом]
@[опционально: тегаем ответственного/наиболее заинтересованного человека в случае отклонения конкретно 
  этой метрики в этом срезе (если такой человек есть)]
   
[график]
'''

import telegram
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandahouse as ph
import pandas as pd
from datetime import timedelta, datetime
#from statsmodels.tsa.holtwinters import ExponentialSmoothing

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator'
}

my_token = '7737779209:AAGQd5mQDZBtV0Gyg7zLwiVJWkReJk2GXIY'
bot = telegram.Bot(token=my_token)
chat_id = '-938659451'

default_args = {
    'owner': 'i.arkhincheev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 12, 30)
}


def extract_feed_metrics(): # для расчетов будем учитывать 14 предыдущих дней
    q = """
        SELECT 
            toStartOfFifteenMinutes(f.time) as ts,
            toDate(f.time) as event_date,
            formatDateTime(ts, '%R') as hm,
            uniqExact(f.user_id) as feed_users,
            countIf(f.action='view') as views,
            countIf(f.action='like') as likes,
            (countIf(f.action='like') / countIf(f.action='view'))*100 as CTR
        FROM 
            simulator_20241120.feed_actions as f
        WHERE 
            f.time >=  today() - 14 and f.time < toStartOfFifteenMinutes(now())
        GROUP BY
            ts,
            event_date,
            hm
        ORDER BY
            ts DESC
        """

    result = ph.read_clickhouse(q, connection=connection)
    return result
    
    
def extract_msg_metrics(): # для расчетов будем учитывать 14 предыдущих дней
    q = """
        SELECT 
            toStartOfFifteenMinutes(m.time) as ts,
            toDate(m.time) as event_date,
            formatDateTime(ts, '%R') as hm,
            uniqExact(m.user_id) as msg_users,
            count(m.user_id) as sent_msgs
        FROM 
            simulator_20241120.message_actions as m
        WHERE 
            m.time >=  today() - 14 and m.time < toStartOfFifteenMinutes(now())
        GROUP BY
            ts,
            event_date,
            hm
        ORDER BY
            ts DESC 
        """

    result = ph.read_clickhouse(q, connection=connection)
    return result
 
    
def check_anomaly_with_means(df, metric):
    """
    функция расчета нижней и верхней границы доверительного интервала для рассматриваемой метрики на основе средних значений предыдущих дней

    учитываются значения метрики за предыдущие дни в то же самое время, что и время, для которого производится расчет метрики

    для расчета границы определяется среднее значение среди предыдущих дней, прибавляется (отнимается) стандартное отклонение плюс(минус) среднее значение, умноженное на коэффициент

    алерт сработает при превышении 5% отклонении от границы
    """
    df_interval = df.query('hm == @df.hm[0] and event_date != @df.event_date[0]')
    a = 0.05
    low = round(df_interval[metric].mean() - df_interval[metric].std() - df_interval[metric].mean()*a, 1)
    up = round(df_interval[metric].mean() + df_interval[metric].std() + df_interval[metric].mean()*a, 1)

    if low <= round(df[metric][0], 1) <= up:
        is_alert = 0
        diff = 0
        boundary = ''
        percent = 0
    else: 
        if df[metric][0] < low:
            diff = low - df[metric][0]
            boundary = 'нижней границы'
            percent = diff / low
        else:
            diff = df[metric][0] - up
            boundary = 'верхней границы'
            percent = diff / up
        if percent > 0.05:
            is_alert = 1
        else:
            is_alert = 0

    return is_alert, diff, boundary, percent, low, up
        
        
def check_anomaly_with_IQR(df, metric):
    """
    функция расчета нижней и верхней границы доверительного интервала для рассматриваемой метрики на основе межквартильного размаха

    учитываются значения метрики за предыдущие дни, временной диапазон берется час назад и час вперед (4 по 15 мин.) от расчетного времени 

    для расчета границы определяется 25 и 75 процентили и учитывается межквартильный размах, умноженный на коэффициент

    алерт сработает при превышении 5% отклонении от границы
    """
    t_list = [df.ts[0].strftime('%H:%M')]
    for i in range(1,5):
        prev_15 = (df.ts[0] - timedelta(minutes=i*15)).strftime('%H:%M')
        next_15 = (df.ts[0] + timedelta(minutes=i*15)).strftime('%H:%M')
        t_list.extend((prev_15, next_15))

    df_interval = df.query("hm in @t_list and event_date != @df.event_date[0]").sort_values(by='ts', ascending=False)
    IQR = df_interval[metric].quantile(0.75) - df_interval[metric].quantile(0.25)
    a = 1.0
    low = round(df_interval[metric].quantile(0.25) - a*IQR, 1)
    up = round(df_interval[metric].quantile(0.75) + a*IQR, 1)

    if low <= round(df[metric][0], 1) <= up:
        is_alert = 0
        diff = 0
        boundary = ''
        percent = 0
    else: 
        if df[metric][0] < low:
            diff = low - df[metric][0]
            boundary = 'нижней границы'
            percent = diff / low
        else:
            diff = df[metric][0] - up
            boundary = 'верхней границы'
            percent = diff / up
        if percent > 0.05:
            is_alert = 1
        else:
            is_alert = 0    

    return is_alert, diff, boundary, percent, low, up
       
    
'''       
def check_anomaly_with_holtwinters(df, metric):
    """
    функция расчета нижней и верхней границы доверительного интервала для рассматриваемой метрики на основе прогнозирования временного ряда методом тройного экспоненциального сглаживания, или модели Хольта-Винтерса (Holt-Winters)

    модель тренируется на всем датафрейме за исключением самой поздней точки временного отрезка (та, за которую мы расчитываем метрику), значения этой последней точки мы и прогнозируем

    модель учитывает сезонность, у нас она ежедневная, т.е. 4 отрезка по 15 мин. * 24 часа

    также модель симулирует 1 значение, но делает это с 10 попытками, это нужно нам для построения доверительного интервала

    для расчета границ доверительного интервала берется 97,5 и 2,5 процентили из пробных предсказаний  

    алерт сработает при превышении 5% отклонении от границы
    """
    forecast_df = pd.DataFrame({'ts':df.ts[0]}, index=[0])
    forecast_df.set_index('ts', inplace=True)
    forecast_df.index.freq = '15T'
    train_df = df.query('ts < @df.ts[0]').sort_values(by='ts')
    train_df.set_index('ts', inplace=True)
    train_df.index.freq = '15T'

    model_holt_winters = ExponentialSmoothing(train_df[metric], seasonal='mul', seasonal_periods=4*24).fit()
    forecast_df = model_holt_winters.simulate(nsimulations=1, repetitions=10)
    low = round(forecast_df.quantile(q=0.025, axis='columns')[0], 1)
    up = round(forecast_df.quantile(q=0.975, axis='columns')[0], 1)

    if low <= round(df[metric][0], 1) <= up:
        is_alert = 0
        diff = 0
        boundary = ''
        percent = 0
    else: 
        if df[metric][0] < low:
            diff = low - df[metric][0]
            boundary = 'нижней границы'
            percent = diff / low
        else:
            diff = df[metric][0] - up
            boundary = 'верхней границы'
            percent = diff / up
        if percent > 0.05:
            is_alert = 1
        else:
            is_alert = 0

    return is_alert, diff, boundary, percent, low, up
'''

        
def make_and_send_msg(chat_id, metric, ts, current_value, boundary, percent, algoritm):
    msg = f"""
<i>{ts:%d.%m.%Y %H:%M}ч</i>    
<tg-emoji emoji-id="5368324170671202286">⚠️</tg-emoji> Метрика <b><u>'{metric}'</u></b>:

Текущее значение = {round(current_value, 1)} 
Алгоритм детектирования: <i>{algoritm}</i>
Отклонение от {boundary} на <b>{percent:.0%}</b>
           """
    bot.sendMessage(chat_id=chat_id, text=msg, parse_mode='HTML')
            
            
def draw_and_send_graph(chat_id, df, metric, metric_name, up_list, low_list):
    plt.figure(figsize=(20, 10))
    plt.plot(df['ts'], df[metric], color='#4374B3', linewidth=6, marker='o', markersize=8, markerfacecolor='orange', label='actual_data')
    plt.title(f"{metric_name} в {df.hm[0]}ч на протяжении предыдущих 15 дней", fontsize=20, pad=20, ha='center')
    plt.ylabel(metric_name, fontsize=12, labelpad=10)
    plt.xlabel('')

    if df[metric].iloc[-1] > 1000: k = 0.01
    else: k = 0.005

    if metric == 'CTR':
        plt.text(df['ts'].min()-timedelta(hours=5),df[metric].iloc[-1]+df[metric].iloc[-1]*0.005,f'{df[metric].iloc[-1]:.1f}',color='gray', ha='center', fontsize=10)
        plt.text(df['ts'].max()+timedelta(hours=5),df[metric].iloc[0]+df[metric].iloc[0]*0.005,f'{df[metric].iloc[0]:.1f}',color='gray', ha='center', fontsize=10)
    else:
        plt.text(df['ts'].min()-timedelta(hours=5),df[metric].iloc[-1]+df[metric].iloc[-1]*k,f'{df[metric].iloc[-1]:.0f}',color='gray', ha='center', fontsize=10)
        plt.text(df['ts'].max()+timedelta(hours=5),df[metric].iloc[0]+df[metric].iloc[0]*k,f'{df[metric].iloc[0]:.0f}',color='gray', ha='center', fontsize=10)

    for up in up_list:
        plt.axhline(up['up'], color=up['color'], linestyle=up['linestyle'], label=up['label'], linewidth=3)

    for low in low_list:
        plt.axhline(low['low'], color=low['color'], linestyle=low['linestyle'], label=low['label'], linewidth=3)

    plt.legend()
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = f'alert_{metric}.png'
    plt.close()

    bot.sendPhoto(chat_id=chat_id, photo=plot_object)


@dag(default_args=default_args, schedule_interval='*/15 * * * *', catchup=False)
def check_anomaly_in_metrics_i_arkhincheev():
    
    @task()
    def run_anomaly_checks():
        df_feed = extract_feed_metrics()
        df_msg = extract_msg_metrics()
        merged_df = pd.merge(df_feed, df_msg, how='outer', on=['ts', 'event_date', 'hm'])
        
        metrics = ['feed_users', 'msg_users', 'views', 'likes', 'CTR', 'sent_msgs']
        
        for metric in metrics:  
            is_alert_means, diff_means, boundary_means, percent_means, low_means, up_means = check_anomaly_with_means(merged_df, metric=metric)
            is_alert_IQR, diff_IQR, boundary_IQR, percent_IQR, low_IQR, up_IQR = check_anomaly_with_IQR(merged_df, metric=metric)
            #is_alert_ML, diff_ML, boundary_ML, percent_ML, low_ML, up_ML = check_anomaly_with_holtwinters(merged_df, metric=metric)

            if (is_alert_means + is_alert_IQR) >= 1:
                up = []
                low = []
                if is_alert_means:
                    up.append({'up':up_means, 'color': '#4d9078', 'linestyle': ':','label': 'upper_means'})
                    low.append({'low':low_means, 'color': '#4d9078', 'linestyle': ':','label': 'lower_means'})
                    boundary = boundary_means
                    percent = percent_means
                    main_algoritm = 'Сравнение средних значений за аналогичный промежуток времени прошлых дней'
                    
                if is_alert_IQR:
                    up.append({'up':up_IQR, 'color': '#FE938C', 'linestyle': '--', 'label': 'upper_IQR'})
                    low.append({'low':low_IQR, 'color': '#FE938C', 'linestyle': '--', 'label': 'lower_IQR'})
                    boundary = boundary_IQR
                    percent = percent_IQR
                    main_algoritm = 'Расчет межквартильного размаха за соседние интервалы прошлых дней'
        
                '''
                if is_alert_ML:
                    up.append({'up':up_ML, 'color': '#7a69ef', 'linestyle': '-.', 'label': 'upper_ML'})
                    low.append({'low':low_ML, 'color': '#7a69ef', 'linestyle': '-.', 'label': 'lower_ML'})
                    boundary = boundary_ML
                    percent = percent_ML 
                    main_algoritm = 'Предсказание путем экспоненциального сглаживания(Хольт-Винтерс)'
                '''    
                if metric == 'feed_users': metric_name = 'Кол-во активных пользователей ленты новостей'
                elif metric == 'msg_users': metric_name = 'Кол-во активных пользователей мессенджера'
                elif metric == 'views': metric_name = 'Кол-во просмотров'
                elif metric == 'likes': metric_name = 'Кол-во лайков'
                elif metric == 'CTR': metric_name = 'CTR (%)'
                elif metric == 'sent_msgs': metric_name = 'Кол-во отправленных сообщений'

                make_and_send_msg(chat_id=chat_id, metric=metric_name, ts = merged_df['ts'][0], current_value = merged_df[metric][0], boundary=boundary, percent=percent, algoritm=main_algoritm)
                draw_and_send_graph(chat_id=chat_id, df=merged_df.query('hm == @merged_df.hm[0]'), metric=metric, metric_name=metric_name, up_list=up, low_list=low)
    
    
    run_checks = run_anomaly_checks()
    
check_anomaly_in_metrics_dag = check_anomaly_in_metrics_i_arkhincheev()