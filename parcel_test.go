package main

import (
	"database/sql"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	// randSource источник псевдо случайных чисел.
	// Для повышения уникальности в качестве seed
	// используется текущее время в unix формате (в виде числа)
	randSource = rand.NewSource(time.Now().UnixNano())
	// randRange использует randSource для генерации случайных чисел
	randRange = rand.New(randSource)
)

// getTestParcel возвращает тестовую посылку
func getTestParcel() Parcel {
	return Parcel{
		Client:    1000,
		Status:    ParcelStatusRegistered,
		Address:   "test",
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}
}

// TestAddGetDelete проверяет добавление, получение и удаление посылки
func TestAddGetDelete(t *testing.T) {
	// prepare
	db, err := sql.Open("sqlite", ":memory:") // настройте подключение к БД
	require.NoError(t, err)
	defer db.Close()

	store := NewParcelStore(db)
	parcel := getTestParcel()

	_, err = db.Exec("CREATE TABLE parcel (number INTEGER PRIMARY KEY AUTOINCREMENT, client INTEGER, status TEXT, address TEXT, created_at TEXT)")
	require.NoError(t, err)

	// add
	// добавьте новую посылку в БД, убедитесь в отсутствии ошибки и наличии идентификатора
	id, err := store.Add(parcel)
	require.NoError(t, err)
	require.NotZero(t, id)

	// get
	// получите только что добавленную посылку, убедитесь в отсутствии ошибки
	// проверьте, что значения всех полей в полученном объекте совпадают со значениями полей в переменной parcel
	retrievedParcel, err := store.Get(id)
	assert.NoError(t, err)
	assert.Equal(t, parcel.Client, retrievedParcel.Client)
	assert.Equal(t, parcel.Status, retrievedParcel.Status)
	assert.Equal(t, parcel.Address, retrievedParcel.Address)

	// delete
	// удалите добавленную посылку, убедитесь в отсутствии ошибки
	err = store.Delete(id)
	require.NoError(t, err)

	// проверьте, что посылку больше нельзя получить из БД
	_, err = store.Get(id)
	require.Error(t, err)
}

// TestSetAddress проверяет обновление адреса
func TestSetAddress(t *testing.T) {
	// prepare
	db, err := sql.Open("sqlite", ":memory:") // настройте подключение к БД
	require.NoError(t, err)
	defer db.Close()

	store := NewParcelStore(db)
	_, err = db.Exec("CREATE TABLE parcel (number INTEGER PRIMARY KEY AUTOINCREMENT, client INTEGER, status TEXT, address TEXT, created_at TEXT)")
	require.NoError(t, err)

	// add
	// добавьте новую посылку в БД, убедитесь в отсутствии ошибки и наличии идентификатора
	parcel := getTestParcel()
	id, err := store.Add(parcel)
	require.NoError(t, err)

	// set address
	// обновите адрес, убедитесь в отсутствии ошибки
	newAddress := "new test address"
	err = store.SetAddress(id, newAddress)
	require.NoError(t, err)

	// check
	// получите добавленную посылку и убедитесь, что адрес обновился
	retrievedParcel, err := store.Get(id)
	assert.NoError(t, err)
	assert.Equal(t, newAddress, retrievedParcel.Address)
}

// TestSetStatus проверяет обновление статуса
func TestSetStatus(t *testing.T) {
	// prepare
	db, err := sql.Open("sqlite", ":memory:") // настройте подключение к БД
	require.NoError(t, err)
	defer db.Close()

	store := NewParcelStore(db)
	_, err = db.Exec("CREATE TABLE parcel (number INTEGER PRIMARY KEY AUTOINCREMENT, client INTEGER, status TEXT, address TEXT, created_at TEXT)")
	require.NoError(t, err)

	// add
	// добавьте новую посылку в БД, убедитесь в отсутствии ошибки и наличии идентификатора
	parcel := getTestParcel()
	id, err := store.Add(parcel)
	require.NoError(t, err)

	// set status
	// обновите статус, убедитесь в отсутствии ошибки
	newStatus := ParcelStatusSent
	err = store.SetStatus(id, newStatus)
	require.NoError(t, err)

	// check
	// получите добавленную посылку и убедитесь, что статус обновился
	retrievedParcel, err := store.Get(id)
	assert.NoError(t, err)
	assert.Equal(t, newStatus, retrievedParcel.Status)

}

// TestGetByClient проверяет получение посылок по идентификатору клиента
func TestGetByClient(t *testing.T) {
	// prepare
	db, err := sql.Open("sqlite", ":memory:") // настройте подключение к БД
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE parcel (number INTEGER PRIMARY KEY AUTOINCREMENT, client INTEGER, status TEXT, address TEXT, created_at TEXT)")
	require.NoError(t, err)

	store := NewParcelStore(db)

	parcels := []Parcel{
		getTestParcel(),
		getTestParcel(),
		getTestParcel(),
	}

	client := randRange.Intn(10_000_000)
	for i := range parcels {
		parcels[i].Client = client
	}

	for i := 0; i < len(parcels); i++ {
		id, err := store.Add(parcels[i])
		require.NoError(t, err)
		parcels[i].Number = id
	}

	// get by client
	storedParcels, err := store.GetByClient(client)
	require.NoError(t, err)

	assert.ElementsMatch(t, parcels, storedParcels)

}
