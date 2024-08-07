package auction

import (
	"context"
	"fullcycle-auction_go/configuration/logger"
	"fullcycle-auction_go/internal/entity/auction_entity"
	"fullcycle-auction_go/internal/internal_error"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
)

type AuctionEntityMongo struct {
	Id          string                          `bson:"_id"`
	ProductName string                          `bson:"product_name"`
	Category    string                          `bson:"category"`
	Description string                          `bson:"description"`
	Condition   auction_entity.ProductCondition `bson:"condition"`
	Status      auction_entity.AuctionStatus    `bson:"status"`
	Timestamp   int64                           `bson:"timestamp"`
}
type AuctionRepository struct {
	Collection *mongo.Collection
}

type auctionBatch []auction_entity.Auction

type AuctionUpdateBatch struct {
	AuctionRepository AuctionRepository

	timer               *time.Timer
	maxBatchSize        int
	batchInsertInterval time.Duration
	bidChannel          chan bid_entity.Bid
}

func NewAuctionRepository(database *mongo.Database) *AuctionRepository {
	return &AuctionRepository{
		Collection: database.Collection("auctions"),
	}
}

func (ar *AuctionRepository) CreateAuction(
	ctx context.Context,
	auctionEntity *auction_entity.Auction) *internal_error.InternalError {
	auctionEntityMongo := &AuctionEntityMongo{
		Id:          auctionEntity.Id,
		ProductName: auctionEntity.ProductName,
		Category:    auctionEntity.Category,
		Description: auctionEntity.Description,
		Condition:   auctionEntity.Condition,
		Status:      auctionEntity.Status,
		Timestamp:   auctionEntity.Timestamp.Unix(),
	}
	_, err := ar.Collection.InsertOne(ctx, auctionEntityMongo)
	if err != nil {
		logger.Error("Error trying to insert auction", err)
		return internal_error.NewInternalServerError("Error trying to insert auction")
	}

	return nil
}

func CalculateAuctionTime(auctionTimestamp time.Time) time.Duration {
	return time.Since(auctionTimestamp)
}

func prepareBatch() {
	maxSizeInterval := getMaxBatchSizeInterval()
	maxBatchSize := getMaxBatchSize()

	bidUseCase := &BidUseCase{
		BidRepository:       bidRepository,
		maxBatchSize:        maxBatchSize,
		batchInsertInterval: maxSizeInterval,
		timer:               time.NewTimer(maxSizeInterval),
		bidChannel:          make(chan bid_entity.Bid, maxBatchSize),
	}

	bidUseCase.triggerCreateRoutine(context.Background())

}

func monitorAuctionExpiration(ctx context.Context, ar *AuctionRepository) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
}

func closeAuction(ctx context.Context, ar *AuctionRepository, id string) {
	filter := bson.M{"_id": id}
	update := bson.M{"$set": bson.M{"status": auction_entity.Completed}}

	result, err := ar.Collection.UpdateOne(ctx, filter, update)
}

func (au *AuctionUseCase) triggerCreateRoutine(ctx context.Context) {
	go func() {
		defer close(au.bidChannel)

		for {
			select {
			case bidEntity, ok := <-au.bidChannel:
				if !ok {
					if len(auctionBatch) > 0 {
						if err := au.BidRepository.CreateBid(ctx, auctionBatch); err != nil {
							logger.Error("error trying to process bid batch list", err)
						}
					}
					return
				}

				auctionBatch = append(auctionBatch, auctionEntity)

				if len(auctionBatch) >= au.maxBatchSize {
					if err := au.BidRepository.CreateBid(ctx, auctionBatch); err != nil {
						logger.Error("error trying to process bid batch list", err)
					}

					auctionBatch = nil
					au.timer.Reset(au.batchInsertInterval)
				}
			case <-au.timer.C:
				if err := au.BidRepository.CreateBid(ctx, auctionBatch); err != nil {
					logger.Error("error trying to process bid batch list", err)
				}
				auctionBatch = nil
				au.timer.Reset(au.batchInsertInterval)
			}
		}
	}()

}

func fetchActiveAuctions(ctx context.Context, ar *AuctionRepository) *internal_error.InternalError {
	var status auction_entity.AuctionStatus = auction_entity.Active
	auctions, err := ar.FindAuctions(ctx, status, "", "")
	if err != nil {
		logger.Error("Error trying to fetch active auctions", err)
		return internal_error.NewInternalServerError("Error trying to fetch active auctions")
	}
	for _, auction := range auctions {
		if CalculateAuctionTime(auction.Timestamp) > time.Second {
			prepareBatch()
		}
	}
}
