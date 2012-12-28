/**
 * $URL: svn+ssh://tserver/var/lib/svn/cps/trunk/src/cpp/lib/database/Connection.h $
 * $Author: onion $
 * $Date: 2012-11-12 15:58:57 +0400 (Пн., 12 нояб. 2012) $
 * $Rev: 374 $
 */
/*
 * Connection.h
 *
 *  Created on: 15.07.2012
 *      Author: romanenko
 */

#ifndef CONNECTION_H_
#define CONNECTION_H_
#include <QSqlDatabase>
#include <QSqlQuery>
#include <QSqlError>
#include <QSqlDriver>
#include <QSqlRecord>
#include <QSqlField>
#include "log/Log.h"
#include "exception/Message.h"
#include "exception/Sql.h"
#include <QVariant>
namespace DB {

class Connection {
public:
	Connection(const QString& ident,const QString& driver, const QString& dbHost,
			const QString& dbName, const QString& dbUser,
			const QString& dbPassword,int dbPort);
	virtual ~Connection();
	void lock();
	void unlock();
	inline bool isLocked() const { return _threadId != 0; }
	inline pthread_t threadID() const { return _threadId; }
	void exec(QSqlQuery& sql);
	void exec(const char* sql);
	void exec(const QString& sql);


	/**
	 * возвращает список значений, взятых из первого столбца всех строк набора результатов
	 */
	template <typename T>
	void fetchCol(QSqlQuery& sql,QList<T>& result) {
		if(!sql.exec()) {
				throw exc::Sql(sql);
		}
		logQuery(sql);
		while (sql.next()) {
			const QSqlRecord& rec = sql.record();
			result.append(rec.value(0).value<T>());
		}

	}

	template <typename T>
	QList<T> fetchCol(QSqlQuery& sql) {
		QList<T> result;
		fetchCol(sql,result);
		return result;
	}

	template <typename T>
	QList<T> fetchCol(const char* sql) {
		QList<T> result;
		QSqlQuery qs = createQuery();
		qs.prepare (sql);
		fetchCol(qs,result);
		return result;
	}

	template <typename T>
	QList<T> fetchCol(const QString&  sql) {
		QList<T> result;
		QSqlQuery qs = createQuery();
		qs.prepare (sql);
		fetchCol(qs,result);
		return result;
	}

	/*
	 * Возвращает значение первого столбца первой строки
	 */
	template <typename T>
	T fetchOne(QSqlQuery& sql, bool* ok = 0) {
		T result;

		if (! sql.exec()) {
			throw exc::Sql(sql);
		}

		logQuery(sql);
		const bool status = sql.size() > 0;

		if (ok != 0)
			*ok = status;

		if (status) {
			sql.next();
			result = sql.record().value(0).value<T>();
		}

		return result;
	}

	template <typename T>
	T fetchOne(const QString&  sql, bool* ok = 0) {
		QSqlQuery qs = createQuery();
		qs.prepare (sql);
		return fetchOne<T>(qs, ok);
	}

	template <typename T>
	T fetchOne(const char* sql, bool* ok = 0) {
		QSqlQuery qs = createQuery();
		qs.prepare (sql);
		return fetchOne<T>(qs, ok);
	}


	/*Возвращает первую строку*/
	void fetchRow(QSqlQuery& sql,QVariantMap& res);
	QVariantMap fetchRow(const QString&  sql);
	QVariantMap fetchRow(const char* sql);
	QVariantMap fetchRow(QSqlQuery& sql);






	void query(QSqlQuery& sql,QList<QVariant>& result);
	QList<QVariant> query(const char* sql);
	QList<QVariant> query(QSqlQuery& sql);
	QList<QVariant> query(const QString&  sql);

	/* Алиасы query	 */
	void fetchAll(QSqlQuery& sql,QList<QVariant>& result);
	QList<QVariant> fetchAll(const char* sql);
	QList<QVariant> fetchAll(QSqlQuery& sql);
	QList<QVariant> fetchAll(const QString&  sql);


	const QSqlDatabase& sqlDatabase() {return _conn; }
	QSqlQuery createQuery() const { return QSqlQuery(_conn);}
	QSqlQuery createQuery(const QString& sql) { return QSqlQuery(sql, _conn); }
	const int referenceCount() const { return _countRef; }
	inline bool inTransaction() const { return _countBegins > 0; }
	int countBegins() const { return _countBegins; }
	void beginTransaction();
	void commitTransaction();
	void rollbackTransaction();

	bool isValid() const { return !_invalid;}
	QString name() const { return _conn.connectionName(); }
	/*void beginTransaction();
	void commit();
	void rollback() = 0;*/
	//inline bool inTransaction() const { return in_transaction_; }
private:
	pthread_t _threadId;
	int _countRef;
	int _countBegins;
	QSqlDatabase _conn;
	int  _retryCount;
	bool _invalid;
	void open();
	void close();
	void logQuery(const QSqlQuery& query);
	bool reconnect();
	bool tryRetry(const QSqlError& err);
	void cpQuery(const QSqlQuery& query,QSqlQuery& newq);

};

} /* namespace DB */
#endif /* CONNECTION_H_ */
