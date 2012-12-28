/**
 * $URL: svn+ssh://tserver/var/lib/svn/cps/trunk/src/cpp/lib/database/ConnectionManager.cpp $
 * $Author: romanenko $
 * $Date: 2012-08-28 15:18:13 +0400 (Вт., 28 авг. 2012) $
 * $Rev: 288 $
 */
/*
 * ConnectionManager.cpp
 *
 *  Created on: 15.07.2012
 *      Author: romanenko
 */

#include "ConnectionManager.h"

#include <QMutexLocker>
#include <QStringList>

#include "Connection.h"
#include "exception/Message.h"
#include "log/Log.h"



namespace DB {
const int MAX_RETRY_GET_CONN_COUNT = 30;

QMap<QString, ConnectionManager*> ConnectionManager::_instances;
QMutex dmutex(QMutex::Recursive);


ConnectionManager::~ConnectionManager() {
	// TODO Auto-generated destructor stub
}

ConnectionManager* ConnectionManager::instance(const QString& ident) {
	return instance(ident.toUtf8().constData());
}

ConnectionManager* ConnectionManager::instance(const char* ident) {
	if (_instances.size() == 0) {
		throw exc::Message("DB::ConnectionManager was not initialized");
	}
	else if (!_instances.contains(ident)) {
		throw exc::Message(
			QString("DB::ConnectionManager \"%1\" was not initialized").arg(ident)
		);
	}

	return _instances[ident];
}


void ConnectionManager::init(const QVariantMap& settings) {
	const int size = settings.value("database/size").toInt();

	for (int i = 1; i <= size; ++i) {
		const QString ident = 
			settings.value(QString("database/%1/ident").arg(i), "default").toString();

		ConnectionManager* inst = new ConnectionManager(
				ident,
				settings.value(QString("database/%1/driver").arg(i)).toString(),
				settings.value(QString("database/%1/host").arg(i)).toString(),
				settings.value(QString("database/%1/name").arg(i)).toString(),
				settings.value(QString("database/%1/user").arg(i)).toString(),
				settings.value(QString("database/%1/password").arg(i)).toString(),
				settings.value(QString("database/%1/port").arg(i), 5432).toInt(),
				settings.value(QString("database/%1/max_count").arg(i), 3).toInt()
			);
		_instances[ident] = inst;

		Log::info(QString("ConnectionManager::init: [%1] [%2@%3:%4] ").arg(inst->_driver).arg(inst->_dbUser).arg(inst->_dbHost).arg(inst->_dbPort));
	}
}


Connection* ConnectionManager::getConnection() {
	Connection* conn = 0;
	int count = 0;
	while(count++ < MAX_RETRY_GET_CONN_COUNT) {

		pthread_t thread_id = pthread_self();
		//Бегаем по подключениям, ищим подключение которое возможно уже было взято в текущем потоке
		{
			QMutexLocker mlocker(&_mutex);
			for (int i = 0; i < _pool.size(); ++i) {
				conn = _pool.at(i);
				//если находим то возвращаем
				if (conn && conn->threadID() == thread_id && conn->isValid()) {
					//увеличивая счетчик ссылок на это подключение в этом треде
					conn->lock();
					//Log::debug(QString("ConnectionManager::getConnection Возвращем то же самое подключение что было ранее залочено thread [%1])").arg(conn->name()));
					return conn;
				}
			}
		}
		//если не нашли прежде взятые в этом потоке, то будем искать первое не залоченное
		{
			QMutexLocker mlocker(&_mutex);
			for (int i = 0; i < _pool.size(); ++i) {
				conn = _pool.at(i);
				if (conn && !conn->isLocked() && conn->isValid() ) {
					//Log::debug(QString("ConnectionManager::getConnection Захват свободного подключения [%1])").arg(conn->name()));
					//таки лочим его
					conn->lock();
					return conn;
				}
			}
		}

	//если тут оказались то нет больше поключений
		{
			QMutexLocker mlocker(&_mutex);
			if(_currentCount < _maxCount) { //если текущее количество не превышает максимальное
				//то создадим новое подключение
				//try {
					conn = new Connection(
							QString("%1_%2").arg(_ident).arg(_currentCount),
							_driver, _dbHost,
							_dbName, _dbUser,
							_dbPassword,_dbPort
					);
					_currentCount++;
					conn->lock();
					_pool.append(conn);
					return conn;
				/*} catch(exc::Message& ex) {
					delete conn;
					throw ex;
				}*/
			} else {
				//удалим первый невалидный
				//Log::warn("Достигнуто максимальное кол-во [%d] доступных подключений к DB попытка [%d]",_maxCount,count);
				/*for (int i = 0; i < _pool.size(); ++i) {
					conn = _pool.at(i);
					if (!conn->isValid() && !conn->isLocked() ) {
						removeConnection(conn);
						break;

					}
				}*/
			}
		}
		//Если нельзя,
			//вздремнем малость и по новой
		sleep(2);
	}
	Log::crit("После %d не смог подключить подключение к базе данных",MAX_RETRY_GET_CONN_COUNT);

	{
		QMutexLocker mlocker(&_mutex);
		for (int i = 0; i < _pool.size(); ++i) {
			conn = _pool.at(i);
			if (!conn->isValid() && !conn->isLocked()) {
				removeConnection(conn);
				break;

			}
		}

	}


	throw exc::Message("Невозможно получить подключение к базе данных");

	return 0;
}


void ConnectionManager::releaseConnection(Connection* conn) {
	// ?! так прсото o_O
	if(conn) {
		conn->unlock();
		Log::debug(QString("Подключение [%1] освобождено").arg(conn->name()));
	}
}

void ConnectionManager::removeConnection(Connection* conn) {
	QMutexLocker mlocker(&_mutex);
	for (int i = 0; i < _pool.size(); ++i) {
		if(conn == _pool.at(i)) {
			Log::debug(QString("Удаление подключения [%1] из пула. Уничтожение объекта подключения").arg(conn->name()));
			_pool.removeAt(i);
			_currentCount--;
			delete conn;
		}
	}
}

} /* namespace DB */
