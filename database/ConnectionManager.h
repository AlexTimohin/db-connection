/**
 * $URL: svn+ssh://tserver/var/lib/svn/cps/trunk/src/cpp/lib/database/ConnectionManager.h $
 * $Author: romanenko $
 * $Date: 2012-08-20 09:09:42 +0400 (Пн., 20 авг. 2012) $
 * $Rev: 261 $
 */
/*
 * ConnectionManager.h
 *
 *  Created on: 15.07.2012
 *      Author: romanenko
 */

#ifndef CONNECTIONMANAGER_H_
#define CONNECTIONMANAGER_H_


#include <QSettings>
#include <QMutex>
#include <QDebug>


namespace DB {

	class Connection;


class ConnectionManager {
public:
	ConnectionManager(
		const QString& ident,
		const QString& driver,
		const QString& dbHost,
		const QString& dbName,
		const QString& dbUser,
		const QString& dbPassword,
		int dbPort,
		int maxCount
	): _ident(ident),
		_driver(driver),
		_dbHost(dbHost),
		_dbName(dbName),
		_dbUser(dbUser),
		_dbPassword(dbPassword),
		_dbPort(dbPort),
		_maxCount(maxCount),
		_currentCount(0),
		_mutex(QMutex::Recursive) {}

	virtual ~ConnectionManager();

	Connection* getConnection();
	static ConnectionManager* instance(const char* ident = "default");
	static ConnectionManager* instance(const QString& ident = "default");
	static void init(const QVariantMap& settings);
	void releaseConnection(Connection* conn);
	void removeConnection(Connection* conn);
	/*QString debugStr() { return QString("ident: [%1] driver: [%2] host: [%3] name: [%4] user:[%5] pass[%6] port[%7]")
			.arg(_ident).arg(_driver).arg(_dbHost).arg(_dbName).arg(_dbUser).arg(_dbPassword).arg(_dbPort);}*/
private:
	int startConnections();

	QList<Connection*> _pool;
	static QMap<QString, ConnectionManager*> _instances;
	QString _ident;
	QString _driver;
	QString _dbHost;
	QString _dbName;
	QString _dbUser;
	QString _dbPassword;
	int _dbPort;
	int _maxCount;
	int _currentCount;
	QMutex _mutex;
};

} /* namespace DB */


#endif /* CONNECTIONMANAGER_H_ */
