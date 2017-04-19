import Sessions

extension Config {
    /// Adds a configurable Sessions instance.
    public mutating func addConfigurable<
        Sessions: SessionsProtocol
    >(sessions: Sessions, name: String) {
        addConfigurable(instance: sessions, unique: "sessions", name: name)
    }
    
    /// Adds a configurable Sessions class.
    public mutating func addConfigurable<
        Sessions: SessionsProtocol & ConfigInitializable
    >(sessions: Sessions.Type, name: String) {
        addConfigurable(class: Sessions.self, unique: "sessions", name: name)
    }
    
    /// Resolves the configured Sessions.
    public func resolveSessions() throws -> SessionsProtocol {
        return try resolve(
            unique: "sessions",
            file: "droplet",
            keyPath: ["sessions"],
            as: SessionsProtocol.self,
            default: MemorySessions.init
        )
    }
}

extension MemorySessions: ConfigInitializable {
    public convenience init(config: Config) throws {
        self.init()
    }
}

extension CacheSessions: ConfigInitializable {
    public convenience init(config: Config) throws {
        let cache = try config.resolveCache()
        self.init(cache)
    }
}

extension SessionsMiddleware: ConfigInitializable {
    public convenience init(config: Config) throws {
        let sessions = try config.resolveSessions()
        self.init(sessions)
    }
}