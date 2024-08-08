package auction

import (
	"context"
	"fmt"
	"fullcycle-auction_go/configuration/logger"
	"fullcycle-auction_go/internal/entity/auction_entity"
	"fullcycle-auction_go/internal/internal_error"
	"fullcycle-auction_go/internal/usecase/bid_usecase"
	"time"

	"go.mongodb.org/mongo-driver/bson"
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

var auctionBatch []auction_entity.Auction

type AuctionBatchManager struct {
	AuctionRepository   *AuctionRepository
	MaxBatchSize        int
	BatchInsertInterval time.Duration
	Batch               chan auction_entity.Auction
	timer               *time.Timer
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

func NewAuctionBatchManager(ar *AuctionRepository) *AuctionBatchManager {
	maxSizeInterval := bid_usecase.GetMaxBatchSizeInterval()
	maxBatchSize := bid_usecase.GetMaxBatchSize()

	return &AuctionBatchManager{
		AuctionRepository:   ar,
		MaxBatchSize:        maxBatchSize,
		BatchInsertInterval: maxSizeInterval,
		timer:               time.NewTimer(maxSizeInterval),
	}
}

func fetchActiveAuctions(ctx context.Context, ar *AuctionRepository, abm *AuctionBatchManager) *internal_error.InternalError {
	var status auction_entity.AuctionStatus = auction_entity.Active
	auctions, err := ar.FindAuctions(ctx, status, "", "")
	if err != nil {
		logger.Error("Error trying to fetch active auctions", err)
		return internal_error.NewInternalServerError("Error trying to fetch active auctions")
	}
	for _, auction := range auctions {
		if CalculateAuctionTime(auction.Timestamp) > time.Hour {
			abm.addAuction(ctx, auction)
		}
	}

	return nil
}

// adds an auction to the batch channel
func (abm *AuctionBatchManager) addAuction(ctx context.Context, auction auction_entity.Auction) {
	abm.Batch <- auction
}

func (abm *AuctionBatchManager) Run(ctx context.Context) {
	go func() {
		defer close(abm.Batch)
		for {
			select {
			case auction, ok := <-abm.Batch:
				if !ok {
					if len(auctionBatch) > 0 {
						if err := abm.processBatch(ctx); err != nil {
							logger.Error("Error processing batch", err)
						}
					}
					return
				}

				auctionBatch = append(auctionBatch, auction)
				if len(auctionBatch) >= abm.MaxBatchSize {
					if err := abm.processBatch(ctx); err != nil {
						logger.Error("Error processing batch", err)
					}

					auctionBatch = nil
					abm.timer.Reset(abm.BatchInsertInterval)
				}
			case <-abm.timer.C:
				if err := abm.processBatch(ctx); err != nil {
					logger.Error("Error processing batch", err)
				}

				auctionBatch = nil
				abm.timer.Reset(abm.BatchInsertInterval)
			}

		}
	}()
}

func (abm *AuctionBatchManager) processBatch(ctx context.Context) *internal_error.InternalError {
	// update the auctions in the batch
	filter := bson.M{"_id": bson.M{"$in": getAuctionIds(auctionBatch)}}
	update := bson.M{"$set": bson.M{"status": auction_entity.Completed}}

	result, err := abm.AuctionRepository.Collection.UpdateMany(ctx, filter, update)
	if err != nil {
		logger.Error("Error updating auction statuses", err)
		return internal_error.NewInternalServerError("Error updating auction statuses")
	}

	logger.Info(fmt.Sprintf("Updated %d auctions", result.ModifiedCount))
	abm.Batch = nil

	return nil
}

func getAuctionIds(auction []auction_entity.Auction) []string {
	var ids []string
	for _, a := range auction {
		ids = append(ids, a.Id)
	}
	return ids
}
