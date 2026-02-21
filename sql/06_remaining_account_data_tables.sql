/*
    Spotify Account Data - Remaining Tables
    Tables for the Account Data files not covered by 03_account_data_tables.sql:
    DuoNewFamily, Identifiers, Payments, UserAddress, UserPrompts,
    UserFestivalsDataForSAR, MessageData, Wrapped2025, YourSoundCapsule

    Run AFTER 03_account_data_tables.sql
*/

SET QUOTED_IDENTIFIER ON;
GO

USE [Spotify];
GO

-- ============================================================
-- Duo / Family Plan
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.DuoFamily') AND type = 'U')
CREATE TABLE dbo.DuoFamily (
    DuoFamilyId     INT IDENTITY(1,1) NOT NULL,
    [Address]       NVARCHAR(500)     NULL,
    CONSTRAINT PK_DuoFamily PRIMARY KEY CLUSTERED (DuoFamilyId)
);
GO

-- ============================================================
-- Identifiers (device/account identifiers)
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.Identifier') AND type = 'U')
CREATE TABLE dbo.Identifier (
    IdentifierId    INT IDENTITY(1,1) NOT NULL,
    IdentifierType  NVARCHAR(100)     NULL,
    IdentifierValue NVARCHAR(500)     NULL,
    CONSTRAINT PK_Identifier PRIMARY KEY CLUSTERED (IdentifierId)
);
GO

-- ============================================================
-- Payments
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.Payment') AND type = 'U')
CREATE TABLE dbo.Payment (
    PaymentId       INT IDENTITY(1,1) NOT NULL,
    PaymentMethod   NVARCHAR(200)     NULL,
    CreationDate    DATE              NULL,
    Country         CHAR(2)           NULL,
    PostalCode      VARCHAR(20)       NULL,
    CONSTRAINT PK_Payment PRIMARY KEY CLUSTERED (PaymentId)
);
GO

-- ============================================================
-- User Address (Scala Map format - parsed into relational rows)
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.UserAddress') AND type = 'U')
CREATE TABLE dbo.UserAddress (
    UserAddressId       INT IDENTITY(1,1) NOT NULL,
    Street              NVARCHAR(300)     NULL,
    City                NVARCHAR(100)     NULL,
    [State]             NVARCHAR(50)      NULL,
    PostalCodeShort     VARCHAR(20)       NULL,
    PostalCodeExtra     VARCHAR(20)       NULL,
    CONSTRAINT PK_UserAddress PRIMARY KEY CLUSTERED (UserAddressId)
);
GO

-- ============================================================
-- User Prompts (AI/recommendation prompts)
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.UserPrompt') AND type = 'U')
CREATE TABLE dbo.UserPrompt (
    UserPromptId        INT IDENTITY(1,1) NOT NULL,
    CreatedTimestamp     DATETIMEOFFSET(3) NULL,
    [Message]           NVARCHAR(1000)    NULL,
    CONSTRAINT PK_UserPrompt PRIMARY KEY CLUSTERED (UserPromptId)
);
GO

-- ============================================================
-- User Festivals
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.UserFestival') AND type = 'U')
CREATE TABLE dbo.UserFestival (
    UserFestivalId          INT IDENTITY(1,1) NOT NULL,
    FestivalId              NVARCHAR(100)     NULL,
    UserId                  VARCHAR(50)       NULL,
    TotalArtistsMatched     INT               NULL,
    MatchPercentile         INT               NULL,
    FestivalPersona         NVARCHAR(100)     NULL,
    TopArtists              NVARCHAR(MAX)     NULL,  -- JSON array of artist names
    TopDiscoveryArtists     NVARCHAR(MAX)     NULL,  -- JSON array of artist names
    CONSTRAINT PK_UserFestival PRIMARY KEY CLUSTERED (UserFestivalId)
);
GO

-- ============================================================
-- Message Data (Spotify in-app chat)
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.ChatConversation') AND type = 'U')
CREATE TABLE dbo.ChatConversation (
    ChatConversationId  INT IDENTITY(1,1) NOT NULL,
    ChatUri             VARCHAR(200)      NOT NULL,
    Members             NVARCHAR(MAX)     NULL,  -- JSON array of usernames
    CONSTRAINT PK_ChatConversation PRIMARY KEY CLUSTERED (ChatConversationId)
);
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.ChatMessage') AND type = 'U')
CREATE TABLE dbo.ChatMessage (
    ChatMessageId       INT IDENTITY(1,1) NOT NULL,
    ChatConversationId  INT               NOT NULL,
    MessageTime         DATETIMEOFFSET(3) NULL,
    SenderUsername      NVARCHAR(200)     NULL,
    [Message]           NVARCHAR(MAX)     NULL,
    MessageUri          VARCHAR(200)      NULL,
    CONSTRAINT PK_ChatMessage PRIMARY KEY CLUSTERED (ChatMessageId),
    CONSTRAINT FK_ChatMessage_Conversation FOREIGN KEY (ChatConversationId) REFERENCES dbo.ChatConversation(ChatConversationId)
);
GO

-- ============================================================
-- Wrapped 2025 (deeply nested - stored as structured sections)
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.Wrapped') AND type = 'U')
CREATE TABLE dbo.Wrapped (
    WrappedId           INT IDENTITY(1,1) NOT NULL,
    [Year]              INT               NOT NULL,
    SectionName         VARCHAR(50)       NOT NULL,
    SectionData         NVARCHAR(MAX)     NOT NULL,  -- JSON for each section
    CONSTRAINT PK_Wrapped PRIMARY KEY CLUSTERED (WrappedId)
);
GO

-- ============================================================
-- Sound Capsule - Weekly Stats
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.SoundCapsuleStat') AND type = 'U')
CREATE TABLE dbo.SoundCapsuleStat (
    SoundCapsuleStatId  INT IDENTITY(1,1) NOT NULL,
    WeekDate            DATE              NULL,
    StreamCount         INT               NULL,
    SecondsPlayed       INT               NULL,
    TopTracks           NVARCHAR(MAX)     NULL,  -- JSON array
    TopArtists          NVARCHAR(MAX)     NULL,  -- JSON array
    TopGenres           NVARCHAR(MAX)     NULL,  -- JSON array
    CONSTRAINT PK_SoundCapsuleStat PRIMARY KEY CLUSTERED (SoundCapsuleStatId)
);
GO

-- ============================================================
-- Sound Capsule - Weekly Highlights
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.SoundCapsuleHighlight') AND type = 'U')
CREATE TABLE dbo.SoundCapsuleHighlight (
    SoundCapsuleHighlightId INT IDENTITY(1,1) NOT NULL,
    WeekDate                DATE              NULL,
    HighlightType           VARCHAR(50)       NULL,
    HighlightData           NVARCHAR(MAX)     NULL,  -- JSON for the type-specific data
    CONSTRAINT PK_SoundCapsuleHighlight PRIMARY KEY CLUSTERED (SoundCapsuleHighlightId)
);
GO

PRINT 'Remaining Account Data tables created successfully.';
GO
