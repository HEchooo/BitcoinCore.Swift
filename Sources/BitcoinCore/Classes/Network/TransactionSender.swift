import Foundation
import Combine
import QuartzCore
import HsToolKit

internal extension NSNotification.Name {
    static let transactionSendStartNotification = NSNotification.Name("BtcTransactionSendStartNotification")
}

class TransactionSender {
    static let minConnectedPeersCount = 2

    private var cancellables = Set<AnyCancellable>()

    private let transactionSyncer: ITransactionSyncer
    private let initialBlockDownload: IInitialDownload
    private let peerManager: IPeerManager
    private let storage: IStorage
    private let timer: ITransactionSendTimer
    private let logger: Logger?
    private let queue: DispatchQueue

    private let maxRetriesCount: Int
    private let retriesPeriod: Double // seconds

    init(transactionSyncer: ITransactionSyncer, initialBlockDownload: IInitialDownload, peerManager: IPeerManager, storage: IStorage, timer: ITransactionSendTimer,
         logger: Logger? = nil, queue: DispatchQueue = DispatchQueue(label: "io.horizontalsystems.bitcoin-core.transaction-sender", qos: .background),
         maxRetriesCount: Int = 3, retriesPeriod: Double = 60) {
        self.transactionSyncer = transactionSyncer
        self.initialBlockDownload = initialBlockDownload
        self.peerManager = peerManager
        self.storage = storage
        self.timer = timer
        self.logger = logger
        self.queue = queue
        self.maxRetriesCount = maxRetriesCount
        self.retriesPeriod = retriesPeriod
    }

    private func peersToSendTo() -> [IPeer] {
        let syncedPeers = initialBlockDownload.syncedPeers
        guard let freeSyncedPeer = syncedPeers.sorted(by: { !$0.ready && $1.ready }).first else {
            return []
        }

        guard peerManager.totalPeersCount >= TransactionSender.minConnectedPeersCount else {
            return []
        }

        let sortedPeers = peerManager.readyPeers
                .filter {
                    freeSyncedPeer !== $0
                }
                .sorted { (a: IPeer, b: IPeer) in
                    !syncedPeers.contains(where: { a === $0 }) && syncedPeers.contains(where: { b === $0 })
                }

        if sortedPeers.count == 1 {
            return sortedPeers
        }

        return Array(sortedPeers.prefix(sortedPeers.count / 2))
    }

    private func transactionsToSend(from transactions: [FullTransaction]) -> [FullTransaction] {
        transactions.filter { transaction in
            if let sentTransaction = storage.sentTransaction(byHash: transaction.header.dataHash) {
                return sentTransaction.lastSendTime < CACurrentMediaTime() - self.retriesPeriod
            } else {
                return true
            }
        }
    }

    private func transactionSendSuccess(sentTransaction transaction: FullTransaction) {
        guard let sentTransaction = storage.sentTransaction(byHash: transaction.header.dataHash),
              !sentTransaction.sendSuccess else {
            return
        }

        sentTransaction.retriesCount = sentTransaction.retriesCount + 1
        sentTransaction.sendSuccess = true

        if sentTransaction.retriesCount >= maxRetriesCount {
            transactionSyncer.handleInvalid(fullTransaction: transaction)
            storage.delete(sentTransaction: sentTransaction)
        } else {
            storage.update(sentTransaction: sentTransaction)
        }
    }

    private func transactionSendStart(transaction: FullTransaction) {
        if let sentTransaction = storage.sentTransaction(byHash: transaction.header.dataHash) {
            sentTransaction.lastSendTime = CACurrentMediaTime()
            sentTransaction.sendSuccess = false
            storage.update(sentTransaction: sentTransaction)
        } else {
            storage.add(sentTransaction: SentTransaction(dataHash: transaction.header.dataHash))
        }
        NotificationCenter.default.post(name: .transactionSendStartNotification, object: nil)
    }

    private func send(transactions: [FullTransaction]) {
        let peers = peersToSendTo()
        guard !peers.isEmpty else {
            return
        }

        timer.startIfNotRunning()

        for transaction in transactions {
            transactionSendStart(transaction: transaction)

            for peer in peers {
                peer.add(task: SendTransactionTask(transaction: transaction))
            }
        }
    }

    private func sendPendingTransactions() {
        var transactions = transactionSyncer.newTransactions()

        guard !transactions.isEmpty else {
            timer.stop()
            return
        }

        transactions = transactionsToSend(from: transactions)

        guard !transactions.isEmpty else {
            return
        }

        send(transactions: transactions)
    }

}

extension TransactionSender: ITransactionSender {

    func verifyCanSend() throws {
        if peersToSendTo().isEmpty {
            throw BitcoinCoreErrors.TransactionSendError.peersNotSynced
        }
    }

    func send(pendingTransaction: FullTransaction) {
        queue.async {
            self.send(transactions: [pendingTransaction])
        }
    }

    func transactionsRelayed(transactions: [FullTransaction]) {
        queue.async {
            for transaction in transactions {
                if let sentTransaction = self.storage.sentTransaction(byHash: transaction.header.dataHash) {
                    self.storage.delete(sentTransaction: sentTransaction)
                }
            }
        }
    }

    func subscribeTo(publisher: AnyPublisher<InitialDownloadEvent, Never>) {
        publisher
                .sink { [weak self] event in
                    switch event {
                    case .onAllPeersSynced:
                        self?.queue.async {
                            self?.sendPendingTransactions()
                        }
                    default: ()
                    }
                }
                .store(in: &cancellables)
    }

}

extension TransactionSender: ITransactionSendTimerDelegate {

    func timePassed() {
        queue.async {
            self.sendPendingTransactions()
        }
    }

}

extension TransactionSender: IPeerTaskHandler {

    func handleCompletedTask(peer: IPeer, task: PeerTask) -> Bool {
        switch task {
        case let task as SendTransactionTask:
            queue.async {
                self.transactionSendSuccess(sentTransaction: task.transaction)
            }
            return true

        default: return false
        }
    }

}
