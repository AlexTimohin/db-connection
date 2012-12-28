/**
 * $URL: svn+ssh://tserver/var/lib/svn/cps/trunk/src/cpp/lib/database/ManagedConnection.h $
 * $Author: romanenko $
 * $Date: 2012-08-20 09:09:42 +0400 (Пн., 20 авг. 2012) $
 * $Rev: 261 $
 */
/*
 * ManagedConnection.h
 *
 *  Created on: 15.07.2012
 *      Author: romanenko
 */

#ifndef MANAGEDCONNECTION_H_
#define MANAGEDCONNECTION_H_
#include "database/Connection.h"
#include "database/ConnectionManager.h"
#include "exception/Message.h"
namespace DB {
	class Connection;
	class ManagedConnection {
		Connection* _conn;
		QString _pull;
	public:
		ManagedConnection(const QString& pull = "default"): _pull(pull) {
			_conn = ConnectionManager::instance(pull)->getConnection();
			if(_conn == 0) {
				throw exc::Message(QString("Cann't get connection for db: %1").arg(pull));
			}

		}
		~ManagedConnection() {
			if(_conn != 0 ) {
				ConnectionManager::instance(_pull)->releaseConnection(_conn);
				if(_conn->isValid()) {
					if(_conn->inTransaction() && _conn->referenceCount() == 0) {
						//std::cerr<<"Where commit || rollback on conn:"<<std::hex<<conn_->getThreadID()<<std::endl;
						Log::crit("~ManagedConnection(): После осовобождения всех ссылок, подключение в состоянии транзакции [countBegins: %d]. Делаю Rollback.",_conn->countBegins());
						_conn->rollbackTransaction();
						//_conn->rollback();
					}
				}

			}
		}
	public:
		Connection* operator->() { return _conn; }
		const Connection* operator->() const { return _conn; }
	};
	typedef ManagedConnection DConn;
} /* namespace DB */
#endif /* MANAGEDCONNECTION_H_ */
