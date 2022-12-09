package edu.montana.csci.csci440.model;

import edu.montana.csci.csci440.util.DB;
import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class Track extends Model {

    private Long trackId;
    private Long albumId;
    private Long mediaTypeId;
    private Long genreId;
    private String name;
    private Long milliseconds;
    private Long bytes;
    private BigDecimal unitPrice;
    private String albumTitle;
    private String artistName;
    public static final String REDIS_CACHE_KEY = "cs440-tracks-count-cache";

    public Track() {
        mediaTypeId = 1l;
        genreId = 1l;
        milliseconds  = 0l;
        bytes  = 0l;
        unitPrice = new BigDecimal("0");
    }

    Track(ResultSet results) throws SQLException {
        name = results.getString("Name");
        milliseconds = results.getLong("Milliseconds");
        bytes = results.getLong("Bytes");
        unitPrice = results.getBigDecimal("UnitPrice");
        trackId = results.getLong("TrackId");
        albumId = results.getLong("AlbumId");
        mediaTypeId = results.getLong("MediaTypeId");
        genreId = results.getLong("GenreId");
        artistName = results.getString("ArtistName");
        albumTitle = results.getString("AlbumTitle");
    }

    @Override
    public boolean verify() {
        _errors.clear(); // clear any existing errors
        if (name == null || "".equals(name)) {
            addError("Name can't be null or blank!");
        }
        if (albumId == null || "".equals(albumId)) {
            addError("AlbumId can't be null or blank!");
        }
        if (milliseconds == null || "".equals(milliseconds)) {
            addError("Milliseconds can't be null or blank!");
        }
        if (bytes == null || "".equals(bytes)) {
            addError("Bytes can't be null or blank!");
        }
        if (unitPrice == null || "".equals(unitPrice)) {
            addError("UnitPrice can't be null or blank!");
        }
        if (mediaTypeId == null || "".equals(mediaTypeId)) {
            addError("MediaTypeId can't be null or blank!");
        }
        if (genreId == null || "".equals(genreId)) {
            addError("GenreId can't be null or blank!");
        }
        return !hasErrors();
    }

    @Override
    public boolean create() {
        if (verify()) {
            try (Connection conn = DB.connect();
                 PreparedStatement stmt = conn.prepareStatement(
                         "INSERT INTO tracks (Name, AlbumId,MediaTypeId,GenreId,Milliseconds,Bytes,UnitPrice) VALUES (?, ?, ?, ?, ?, ?, ?)")) {
                stmt.setString(1, this.getName());
                stmt.setLong(2, this.getAlbumId());
                stmt.setLong(3, this.getMediaTypeId());
                stmt.setLong(4, this.getGenreId());
                stmt.setLong(5, this.getMilliseconds());
                stmt.setLong(6, this.getBytes());
                stmt.setBigDecimal(7, this.getUnitPrice());
                stmt.executeUpdate();
                trackId = DB.getLastID(conn);
                Jedis redisClient = new Jedis();
                redisClient.del(REDIS_CACHE_KEY);
                redisClient.hdel(REDIS_CACHE_KEY, getAlbumId().toString());
                return true;
            } catch (SQLException sqlException) {
                throw new RuntimeException(sqlException);
            }
        } else {
            return false;
        }
    }

    public static Track find(long i) {
        try (Connection conn = DB.connect();
             PreparedStatement stmt = conn.prepareStatement("SELECT *, artists.Name AS ArtistName, albums.Title AS AlbumTitle\n" +
                     "FROM tracks \n" +
                     "JOIN albums ON tracks.AlbumId = albums.AlbumId\n" +
                     "JOIN artists on albums.ArtistId = artists.ArtistId\n" +
                     "WHERE TrackId=?")) {
            stmt.setLong(1, i);
            ResultSet results = stmt.executeQuery();
            if (results.next()) {
                return new Track(results);
            } else {
                return null;
            }
        } catch (SQLException sqlException) {
            throw new RuntimeException(sqlException);
        }
    }

    public static Long count() {
        Jedis redisClient = new Jedis(); // use this class to access redis and create a cache
        String current_hash_value = redisClient.get(REDIS_CACHE_KEY);
        if (current_hash_value != null) {
            return Long.parseLong(current_hash_value);
        } else {
            Long q_count = queryCount();
            redisClient.set(REDIS_CACHE_KEY, Long.toString(q_count));
            return q_count;
        }
    }

    public static long queryCount(){
        try (Connection conn = DB.connect();
             PreparedStatement stmt = conn.prepareStatement("SELECT COUNT(*) as Count FROM tracks")) {
            ResultSet results = stmt.executeQuery();
            if (results.next()) {
                return results.getLong("Count");
            } else {
                throw new IllegalStateException("Should find a count!");
            }
        } catch (SQLException sqlException) {
            throw new RuntimeException(sqlException);
        }
    }

    public Album getAlbum() {
        return Album.find(albumId);
    }

    public MediaType getMediaType() {
        return null;
    }
    public Genre getGenre() {
        return null;
    }
    public List<Playlist> getPlaylists(){
        try (Connection conn = DB.connect();
             PreparedStatement stmt = conn.prepareStatement("SELECT * FROM playlists\n" +
                     "    JOIN playlist_track on playlists.PlaylistId = playlist_track.PlaylistId\n" +
                     "    JOIN tracks on playlist_track.TrackId = tracks.TrackId\n" +
                     "         WHERE tracks.TrackId = ?\n" +
                     "         ORDER BY playlists.Name")) {
            stmt.setLong(1, trackId);
            ResultSet results = stmt.executeQuery();
            List<Playlist> resultList = new LinkedList<>();
            while (results.next()) {
                resultList.add(new Playlist(results));
            }
            return resultList;
        } catch (SQLException sqlException) {
            throw new RuntimeException(sqlException);
        }
    }

    public Long getTrackId() {
        return trackId;
    }

    public void setTrackId(Long trackId) {
        this.trackId = trackId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getMilliseconds() {
        return milliseconds;
    }

    public void setMilliseconds(Long milliseconds) {
        this.milliseconds = milliseconds;
    }

    public Long getBytes() {
        return bytes;
    }

    public void setBytes(Long bytes) {
        this.bytes = bytes;
    }

    public BigDecimal getUnitPrice() {
        return unitPrice;
    }

    public void setUnitPrice(BigDecimal unitPrice) {
        this.unitPrice = unitPrice;
    }

    public Long getAlbumId() {
        return albumId;
    }

    public void setAlbumId(Long albumId) {
        this.albumId = albumId;
    }

    public void setAlbum(Album album) {
        albumId = album.getAlbumId();
    }

    public Long getMediaTypeId() {
        return mediaTypeId;
    }

    public void setMediaTypeId(Long mediaTypeId) {
        this.mediaTypeId = mediaTypeId;
    }

    public Long getGenreId() {
        return genreId;
    }

    public void setGenreId(Long genreId) {
        this.genreId = genreId;
    }

    public String getArtistName() {
        return artistName;
    }

    public String getAlbumTitle() {
        return albumTitle;
    }

    public static List<Track> advancedSearch(int page, int count,
                                             String search, Integer artistId, Integer albumId,
                                             Integer maxRuntime, Integer minRuntime) {
        LinkedList<Object> args = new LinkedList<>();
        String query = "SELECT *, artists.Name AS ArtistName, albums.Title AS AlbumTitle " +
                "FROM tracks " +
                "JOIN albums ON tracks.AlbumId = albums.AlbumId " +
                "JOIN artists on albums.ArtistId = artists.ArtistId\n" +
                "WHERE tracks.name LIKE ?";
        args.add("%" + search + "%");

        // Here is an example of how to conditionally
        if (artistId != null) {
            query += " AND albums.ArtistId=? ";
            args.add(artistId);
        }

        if (albumId != null) {
            query += " AND albums.AlbumId=? ";
            args.add(albumId);
        }

        if (maxRuntime != null) {
            query += " AND Milliseconds < ? ";
            args.add(maxRuntime);
        }

        if (minRuntime != null) {
            query += " AND Milliseconds > ? ";
            args.add(minRuntime);
        }

        //  include the limit (you should include the page too :)
        query += " LIMIT ?";
        args.add(count);

        int offset = (page - 1) * count;
        query += " OFFSET ?";
        args.add(offset);

        try (Connection conn = DB.connect();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            for (int i = 0; i < args.size(); i++) {
                Object arg = args.get(i);
                stmt.setObject(i + 1, arg);
            }
            ResultSet results = stmt.executeQuery();
            List<Track> resultList = new LinkedList<>();
            while (results.next()) {
                resultList.add(new Track(results));
            }
            return resultList;
        } catch (SQLException sqlException) {
            throw new RuntimeException(sqlException);
        }
    }

    public static List<Track> search(int page, int count, String orderBy, String search) {
        int offset = (page - 1) * count;
        String query = "SELECT *, artists.Name AS ArtistName, albums.Title AS AlbumTitle " +
                "FROM tracks " +
                "JOIN albums on tracks.AlbumId = albums.AlbumId " +
                "JOIN artists on albums.ArtistId = artists.ArtistId " +
                "WHERE tracks.Name LIKE ? OR albums.Title LIKE ? " +
                "ORDER BY " + orderBy + " LIMIT ? OFFSET ?";
        search = "%" + search + "%";
        try (Connection conn = DB.connect();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, search);
            stmt.setString(2, search);
            stmt.setInt(3, count);
            stmt.setInt(4, offset);
            ResultSet results = stmt.executeQuery();
            List<Track> resultList = new LinkedList<>();
            while (results.next()) {
                resultList.add(new Track(results));
            }
            return resultList;
        } catch (SQLException sqlException) {
            throw new RuntimeException(sqlException);
        }
    }

    public static List<Track> forAlbum(Long albumId) {
        String query = "SELECT * FROM tracks WHERE AlbumId=?";
        try (Connection conn = DB.connect();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setLong(1, albumId);
            ResultSet results = stmt.executeQuery();
            List<Track> resultList = new LinkedList<>();
            while (results.next()) {
                resultList.add(new Track(results));
            }
            return resultList;
        } catch (SQLException sqlException) {
            throw new RuntimeException(sqlException);
        }
    }

    // Sure would be nice if java supported default parameter values
    public static List<Track> all() {
        return all(0, Integer.MAX_VALUE);
    }

    public static List<Track> all(int page, int count) {
        return all(page, count, "TrackId");
    }

    @Override
    public boolean update() {
        if (verify()) {
            try (Connection conn = DB.connect();
                 PreparedStatement stmt = conn.prepareStatement(
                         "UPDATE tracks SET Name=? WHERE TrackId=?")) {
                stmt.setString(1, this.getName());
                stmt.setLong(2, this.getTrackId());
                stmt.executeUpdate();
                return false;
            } catch (SQLException sqlException) {
                throw new RuntimeException(sqlException);
            }
        } else {
            return false;
        }
    }

    @Override
    public void delete() {
        try (Connection conn = DB.connect();
             PreparedStatement stmt = conn.prepareStatement(
                     "DELETE FROM tracks WHERE TrackID=?")) {
            stmt.setLong(1, this.getTrackId());
            stmt.executeUpdate();
            Jedis redisClient = new Jedis();
            redisClient.del(REDIS_CACHE_KEY);
            redisClient.hdel(REDIS_CACHE_KEY, getAlbumId().toString());
        } catch (SQLException sqlException) {
            throw new RuntimeException(sqlException);
        }
    }

    public static List<Track> all(int page, int count, String orderBy) {
        int offset = (page - 1) * count;
        try (Connection conn = DB.connect();
             PreparedStatement stmt = conn.prepareStatement(
                     "SELECT *, artists.Name AS ArtistName, albums.Title AS AlbumTitle FROM tracks\n" +
                             "JOIN albums ON tracks.AlbumId = albums.AlbumId\n" +
                             "JOIN artists on albums.ArtistId = artists.ArtistId\n" +
                             "ORDER BY " + orderBy +
                             " LIMIT ?\n" +
                             "OFFSET ?"
             )) {
            stmt.setInt(1, count);
            stmt.setInt(2, offset);
            ResultSet results = stmt.executeQuery();
            List<Track> resultList = new LinkedList<>();
            while (results.next()) {
                resultList.add(new Track(results));
            }
            return resultList;
        } catch (SQLException sqlException) {
            throw new RuntimeException(sqlException);
        }
    }

}