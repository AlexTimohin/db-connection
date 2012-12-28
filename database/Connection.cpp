/**
 * $URL: svn+ssh://tserver/var/lib/svn/cps/trunk/src/cpp/lib/database/Connection.cpp $
 * $Author: romanenko $
 * $Date: 2012-08-20 09:09:42 +0400 (Пн., 20 авг. 2012) $
 * $Rev: 261 $
 */
#include "Connection.h"

namespace DB {
const int MAX_RETRY_COUNT = 3;
Connection::Connection(const QString& ident, const QString& driver,
		const QString& dbHost, const QString& dbName, const QString& dbUser,
		const QString& dbPassword, int dbPort) :
		_threadId(0), _countRef(0), _countBegins(0), _retryCount(0),_invalid(false) {
	_conn = QSqlDatabase::addDatabase(driver, ident);
	_conn.setHostName(dbHost);

	_conn.setDatabaseName(dbName);
	_conn.setUserName(dbUser);
	_conn.setPassword(dbPassword);
	_conn.setPort(dbPort),

	open();


}
Connection::~Connection() {
	close();
}
void Connection::open() {
	Log::debug(
			QString("Try connect  %1@%2:%3 %4 %5 %6...").arg(
					_conn.databaseName()).arg(_conn.hostName()).arg(
					_conn.port()).arg(_conn.userName()).arg(_conn.driverName()).arg(
					_conn.connectionName()));
	if (!_conn.open()) {
		/*throw exc::Message(
		 QString("Can not connect to db: %1@%2:%3 %4 %5: %6")
		 .arg(_conn.databaseName()).arg(_conn.hostName())
		 .arg(_conn.port()).arg(_conn.userName()).arg(_conn.driverName())
		 .arg(_conn.lastError().text())
		 );*/
		Log::crit(QString("Connection::open: %1").arg(_conn.lastError().text()));
		throw exc::Message("Ошибка подключение к базе данных");
	}
	if (!_conn.isOpen() || _conn.isOpenError() || !_conn.isValid()) {
		throw exc::Message(_conn.lastError().text());
	}
}

void Connection::lock() {
	_threadId = pthread_self();
	_countRef++;
//	Log::debug("lock:count ref %d",_countRef);
}

void Connection::unlock() {
	_countRef--;
	if (_countRef == 0) {
		_threadId = 0;
	}
//	Log::debug("Unlock:count ref %d",_countRef);

}
void Connection::beginTransaction() {
	bool ok = false;
	if (_countBegins == 0) {
		ok = _conn.transaction();
		if (!ok) {
			Log::crit("Ass");
			throw exc::Message(_conn.lastError().text());
		}
	}
	_countBegins++;
}
void Connection::commitTransaction() {
	if (_countBegins == 1) {
		bool ok = _conn.commit();
		if (!ok) {
			throw exc::Message(_conn.lastError().text());
		}
		Log::debug("COMMIT");
		_countBegins--;
	} else if (_countBegins > 1) {
		Log::debug("COMMIT - не производится [countBegins: %d]", _countBegins);
		_countBegins--;
	} else {
		Log::warn("Транзакция не начиналась, а запрашивается COMMIT");
	}

}
void Connection::rollbackTransaction() {
	if (_countBegins > 0) {
		bool ok = _conn.rollback();
		_countBegins = 0;
		if (!ok) {
			throw exc::Message(_conn.lastError().text());
		}
		Log::debug("ROLLBACK");

	} else {
		Log::warn("Транзакция не начиналась, а запрашивается ROLLBACK");
	}

}

void Connection::close() {
	_conn.close();
	QSqlDatabase::removeDatabase(_conn.connectionName());
}

void Connection::exec(QSqlQuery& sql) {
	if (!sql.exec()) {
		if (tryRetry(sql.lastError())) {
			if(isValid()) {
				QSqlQuery qs = createQuery();
				cpQuery(sql, qs);
				return exec(qs);
			} else {
				throw exc::Message("Ошибка подключения к базе данных");;
			}
		} else
			throw exc::Sql(sql);
	}
	logQuery(sql);

}
void Connection::exec(const char* sql) {
	QSqlQuery qs = createQuery();
	qs.prepare(sql);
	exec(qs);
}
void Connection::exec(const QString& sql) {
	QSqlQuery qs = createQuery();
	qs.prepare(sql);
	exec(qs);
}

QList<QVariant> Connection::query(const char* sql) {
	QList<QVariant> res;
	QSqlQuery qs = createQuery();
	qs.prepare(sql);
	query(qs, res);
	return res;
}
QList<QVariant> Connection::query(const QString& sql) {
	QList<QVariant> res;
	QSqlQuery qs = createQuery();
	qs.prepare(sql);
	query(qs, res);
	return res;
}

QList<QVariant> Connection::query(QSqlQuery& sql) {
	QList<QVariant> res;
	query(sql, res);
	return res;
}
//
QList<QVariant> Connection::fetchAll(const char* sql) {
	QList<QVariant> res;
	QSqlQuery qs = createQuery();
	qs.prepare(sql);
	fetchAll(qs, res);
	return res;
}
QList<QVariant> Connection::fetchAll(const QString& sql) {
	QList<QVariant> res;
	QSqlQuery qs = createQuery();
	qs.prepare(sql);
	fetchAll(qs, res);
	return res;
}

QList<QVariant> Connection::fetchAll(QSqlQuery& sql) {
	QList<QVariant> res;
	fetchAll(sql, res);
	return res;
}

void Connection::logQuery(const QSqlQuery& query) {
	Log::debug(query.lastQuery());
	QMapIterator<QString, QVariant> i(query.boundValues());
	if (i.hasNext()) {
		Log::debug("Params:");
		while (i.hasNext()) {
			i.next();
			Log::debug(
					QString("  [%1]\t\t= %2").arg(i.key()).arg(
							i.value().toString()));

		}
	}
	Log::debug("Rows count: %d", query.size());

}

void Connection::fetchAll(QSqlQuery& sql, QList<QVariant>& result) {
	query(sql, result);
}
/*
 template <typename T>
 QList<T> Connection::fetchCol(QSqlQuery& sql) {
 QList<T> result;
 if(!sql.exec()) {
 throw exc::Sql(sql);
 }
 logQuery(sql);
 while (sql.next()) {
 QSqlRecord rec = sql.record();
 for (int i = 0; i < rec.count(); ++i) {
 result.append(rec.value(i).value<T>());
 }

 }
 return result;

 }
 */

void Connection::query(QSqlQuery& sql, QList<QVariant>& result) {
	if (!sql.exec()) {
		if (tryRetry(sql.lastError())) {
			if(isValid()) {
				QSqlQuery qs = createQuery();
				cpQuery(sql, qs);
				Log::warn("Перезапрашиваем запрос к базе...");
				return query(qs, result);
			} else {
				throw exc::Message("Ошибка подключения к базе данных");;
			}
		} else
			throw exc::Sql(sql);
	} else {
		logQuery(sql);
		while (sql.next()) {
			const QSqlRecord& rec = sql.record();
			QVariantMap data;
			for (int i = 0; i < rec.count(); ++i) {
				data[rec.field(i).name()] = rec.value(i);
			}
			result << data;
		}
	}
}
void Connection::fetchRow(QSqlQuery& sql, QVariantMap& res) {
	if (!sql.exec()) {
		if (tryRetry(sql.lastError())) {
			QSqlQuery qs = createQuery();
			cpQuery(sql, qs);
			Log::warn("Перезапрашиваем запрос к базе...");
			return fetchRow(qs, res);
		} else
			throw exc::Sql(sql);

	}
	logQuery(sql);
	if (sql.size() > 0) {
		sql.next();
		const QSqlRecord& rec = sql.record();
		for (int i = 0; i < rec.count(); ++i) {
			res[rec.field(i).name()] = rec.value(i);
		}
	}
}
QVariantMap Connection::fetchRow(const QString& sql) {
	QVariantMap res;
	QSqlQuery qs = createQuery();
	qs.prepare(sql);
	fetchRow(qs, res);
	return res;

}
QVariantMap Connection::fetchRow(const char* sql) {
	QVariantMap res;
	QSqlQuery qs = createQuery();
	qs.prepare(sql);
	fetchRow(qs, res);
	return res;

}
QVariantMap Connection::fetchRow(QSqlQuery& sql) {
	QVariantMap res;
	fetchRow(sql, res);
	return res;
}

bool Connection::tryRetry(const QSqlError& err) {
	//если постгрисовские гон эвэй
	bool needRecon = false;
	QRegExp mrx("MySQL server has gone away");
	if (err.type() == QSqlError::NoError
			&& qstrcmp(_conn.driver()->handle().typeName(), "PGconn*") == 0) {
		needRecon = true;
	} else if (
	/*(qstrcmp(_conn.driver()->handle().typeName(), "MYSQL*")  == 0)*/
	err.text().contains(mrx)) {
		needRecon = true;
	}
	bool ok = false;
	if (needRecon) {
		Log::err(
				"Похоже что подключение к базе -  GONE AWAY. Буду пытаться переподключиться...");
		bool lol = true;

		while(lol) {
			if (_retryCount >= MAX_RETRY_COUNT) {
				_retryCount = 0;
				_invalid = true;
				Log::crit(
						"Достигнуто максимальное количество попыток переподключиться к базе данных.");
				throw exc::Message("Ошибка при взаимодействии с базаой данных");
			}
			lol = !reconnect();
			if(!lol) {
				ok = true;
				_retryCount = 0;
				break;
			}
			_retryCount++;
			sleep(2);
		}
	}

	//_retryCount
	return ok;
}

bool Connection::reconnect() {
	Log::debug(QString("Закрытие подключения [%1]...").arg(_conn.connectionName()));
	_conn.close();
	bool ok = _conn.open();
	if(ok)
		Log::info("Открытие подключения - успешно");
	else
		Log::crit("Открытие подключения - не успешно");
	return ok;
}
void Connection::cpQuery(const QSqlQuery& query, QSqlQuery& newq) {
	newq.prepare(query.executedQuery());

	QMapIterator<QString, QVariant> i(query.boundValues());
	while (i.hasNext()) {
		i.next();
		newq.bindValue(i.key(), i.value());

	}
	//newq.addBindValue(query.boundValues());
}

} /* namespace DB */
